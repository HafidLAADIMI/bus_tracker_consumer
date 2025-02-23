package com.kafka.consumer.Models;


import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serializable;


@Document
public class Station implements Serializable {

    private String stationName;
    private double latitude;
    private double longitude;

    public Station(String stationName, double latitude, double longitude) {
        this.stationName = stationName;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public String getStationName() {
        return stationName;
    }

    public void setStationName(String stationName) {
        this.stationName = stationName;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }
    public Station() {};
}