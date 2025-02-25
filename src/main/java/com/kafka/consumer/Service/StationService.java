package com.kafka.consumer.Service;


import com.kafka.consumer.Models.BusTracker;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class StationService {
    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final ReactiveMongoTemplate reactiveMongoTemplate;

    public StationService(ReactiveMongoTemplate reactiveMongoTemplate, ReactiveRedisTemplate<String, String> redisTemplate) {
        this.reactiveMongoTemplate = reactiveMongoTemplate;
        this.redisTemplate = redisTemplate;
    }

    // Finding the nearest station
    public Mono<GeoResult<BusTracker>> findNearByStation(double longitude, double latitude) {
        Point point = new Point(longitude, latitude);
        NearQuery nearQuery = NearQuery.near(point).maxDistance(new Distance(5, Metrics.KILOMETERS));
        return reactiveMongoTemplate.geoNear(nearQuery, BusTracker.class).singleOrEmpty();
    }

    // Save last location in mongodb
    public Mono<Void> saveLastLocation(int busId, double longitude, double latitude) {
        return findNearByStation(longitude, latitude)
                .flatMap(newStation -> {
                            if (newStation == null) {
                                return Mono.empty(); // Avoid NullPointerException
                            }
                            ;
                            return reactiveMongoTemplate.findOne(Query.query(Criteria.where("busId").is(busId)), BusTracker.class)
                                    .flatMap(bus -> {
                                        if (!newStation.getContent().getLastStation().equals(bus.getLastStation())) {
                                            bus.setLastStation(newStation.getContent().getLastStation());
                                            bus.setLastStationTime(newStation.getContent().getLastStation());
                                            return reactiveMongoTemplate.save(bus).then();
                                        }
                                        return Mono.empty();
                                    });
                        }
                )
                .onErrorResume(e -> {
                    System.out.println("Error saving last location: " + e.getMessage());
                    return Mono.empty();
                });
    }

    // save the station in redis
    public Mono<Void> saveStation(int busId, double longitude, double latitude) {
        String key = "bus:" + busId;
        String value = longitude + "," + latitude + "," + System.currentTimeMillis();
        return redisTemplate.opsForValue().set(key, value).then();
    }

    // Haversine formula to calculate distance
    private double haversine(double lat1, double lon1, double lat2, double lon2) {
        double R = 6371; // Earth radius in KM
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                        Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }

    // calculating the speed of the bus ; we will use redis instead of mongodb to achieve the best performance
    public Mono<String> calculateSpeed(int busId, double longitude, double latitude) {
        {
            String key = "bus:" + busId;
            return redisTemplate.opsForValue().get(key).defaultIfEmpty("0,0,0").flatMap(oldValue -> {
                String[] parts = oldValue.split(",");
                double oldLatitude = Double.parseDouble(parts[0]);
                double oldLongitude = Double.parseDouble(parts[1]);
                long oldTime = Long.parseLong(parts[2]);
                if (oldLatitude == 0 && oldLongitude == 0 && oldTime == 0) {
                    return Mono.just("Speed: N/A (First entry)");
                }
                ;
                long newTime = System.currentTimeMillis();
                double distance = haversine(oldLatitude, oldLongitude, latitude, longitude);
                double time = (newTime - oldTime);
                if (time == 0) {
                    return Mono.just("Speed: 0");
                }
                double timeInHours = time / 3600000.0;
                double speed = (distance / timeInHours) * 3600000;
                return Mono.just("Speed " + speed + " km/h");
            });
        }
    }


}
