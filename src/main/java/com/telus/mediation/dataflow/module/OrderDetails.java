package com.telus.mediation.dataflow.module;

import java.io.Serializable;

import lombok.Data;

@Data
public class OrderDetails implements Serializable {
    private String orderID;
    private double amount;
    private double profit;
    private int quantity;
    private String category;
    private String subCategory;
}
