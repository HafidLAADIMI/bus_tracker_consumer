package com.kafka.consumer.Controller;

import com.kafka.consumer.Models.BusTracker;
import com.kafka.consumer.Models.Station;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

@RestController
@RequestMapping("/consume")
public class ConsumeBusLocation {

    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final ReactiveMongoTemplate reactiveMongoTemplate;
    private final Sinks.Many<BusTracker> sinkLastStation;
    private final Sinks.Many<String> sinkSpeed;

    // injecting new instance
    public ConsumeBusLocation(ReactiveMongoTemplate reactiveMongoTemplate, Sinks.Many<BusTracker> sinkLastStation, Sinks.Many<String> sinkSpeed, ReactiveRedisTemplate<String, String> redisTemplate, ReactiveRedisOperations<String, String> stationOps) {
        this.reactiveMongoTemplate = reactiveMongoTemplate;
        this.sinkSpeed = sinkSpeed;
        this.sinkLastStation = sinkLastStation;
        this.redisTemplate = redisTemplate;
    }

    // Finding the nearest station
    public Mono<GeoResult<Station>> findNearByStation(double longitude, double latitude) {
        Point point = new Point(longitude, latitude);
        NearQuery nearQuery = NearQuery.near(point).maxDistance(new Distance(5, Metrics.KILOMETERS));
        return reactiveMongoTemplate.geoNear(nearQuery, Station.class).singleOrEmpty();
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
                                        if (!newStation.getContent().getStationName().equals(bus.getLastStation())) {
                                            bus.setLastStation(newStation.getContent().getStationName());
                                            bus.setLastStationTime(newStation.getContent().getStationName());
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



    //  better handling for the backpressure to ensure the non-overwhelmed of mongodb or redis
    @KafkaListener(topics = "bus_location", groupId = "bus_location_id")
    public void consumeLastStation(ConsumerRecord<String, BusTracker> record) {
        if (record.value() == null) {
            System.out.println("Received a null value");
            return;
        }

        BusTracker lastLocation = record.value();

        Mono<Void> process = saveStation(lastLocation.getBusId(),
                lastLocation.getLastStationLocation().getCoordinates().get(0),
                lastLocation.getLastStationLocation().getCoordinates().get(1))
                .then(saveLastLocation(lastLocation.getBusId(),
                        lastLocation.getLastStationLocation().getCoordinates().get(0),
                        lastLocation.getLastStationLocation().getCoordinates().get(1)))
                .then(calculateSpeed(lastLocation.getBusId(),
                        lastLocation.getLastStationLocation().getCoordinates().get(0),
                        lastLocation.getLastStationLocation().getCoordinates().get(1))
                        .doOnNext(speed -> {
                            System.out.println("The speed is " + speed);
                            sinkSpeed.tryEmitNext(speed);
                        }))
                .then(Mono.fromRunnable(() -> sinkLastStation.tryEmitNext(lastLocation)));

        process.subscribe();
    }

    //    @KafkaListener(topics = "bus_location", groupId = "bus_location_id")
//    public void consumeLastStation(ConsumerRecord<String, BusTracker> record) {
//        System.out.println("Consumed message: " + record.value());
//
//        if (record.value() != null) {
//            BusTracker lastLocation = record.value();
//            // save the station in redis
//            saveStation(lastLocation.getBusId(),
//                    lastLocation.getLastStationLocation().getCoordinates().get(0),
//                    lastLocation.getLastStationLocation().getCoordinates().get(1)
//            ).subscribe();
//            // save the last location in mongodb
//            saveLastLocation(lastLocation.getBusId(),
//                    lastLocation.getLastStationLocation().getCoordinates().get(0),
//                    lastLocation.getLastStationLocation().getCoordinates().get(1)
//            ).subscribe();
//            // calculate the speed of the bus
//            calculateSpeed(lastLocation.getBusId(), lastLocation.getLastStationLocation().getCoordinates().get(0), lastLocation.getLastStationLocation().getCoordinates().get(1)).subscribe(speed -> {
//                System.out.println("The speed is " + speed);
//                lastLocation.setLastStationTime(String.valueOf(System.currentTimeMillis()));
//                sinkSpeed.tryEmitNext(speed);
//            });
//            // broadcast the last location and the speed
//            sinkLastStation.tryEmitNext(lastLocation);
//
//
//        } else {
//            System.out.println("Received a null value");
//        }
//    }

}
