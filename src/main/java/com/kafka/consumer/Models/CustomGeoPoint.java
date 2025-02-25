package com.kafka.consumer.Models;

import org.springframework.data.mongodb.core.geo.GeoJsonPoint;

import java.io.Serializable;

public class CustomGeoPoint implements Serializable {
    private double x;
    private double y;

    // Default constructor for Jackson
    public CustomGeoPoint() {}

    public CustomGeoPoint(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    // Convert to GeoJsonPoint when needed
    public GeoJsonPoint toGeoJsonPoint() {
        return new GeoJsonPoint(x, y);
    }


}