package com.rapidminer.pagerank.mongodb.document;

import java.io.*;

public final class Token implements Serializable
{
    private static final long serialVersionUID = 1L;
    private String token;
    private float weight;
    
    public Token(final String token, final Token parent) {
        this.token = token;
        this.weight = parent.getWeight();
    }
    
    public Token(final String token, final float weight) {
        this.token = token;
        this.weight = weight;
    }
    
    public String getToken() {
        return this.token;
    }
    
    @Override
    public String toString() {
        return this.token;
    }
    
    public float getWeight() {
        return this.weight;
    }
}
