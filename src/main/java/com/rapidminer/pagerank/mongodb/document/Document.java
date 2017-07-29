package com.rapidminer.pagerank.mongodb.document;

import com.rapidminer.tools.container.*;
import java.util.*;
import com.rapidminer.operator.*;
import java.lang.reflect.*;
import com.rapidminer.tools.*;

public class Document extends ResultObjectAdapter
{
    private static final long serialVersionUID = 7719788736766297420L;
    private String text;
    private List<Token> tokenSequence;
    private Map<String, Pair<Object, Integer>> metaData;
    private String labelMetaDataKey;
    
    public Document(final List<Token> tokenSequence) {
        this.tokenSequence = new ArrayList<Token>();
        this.metaData = new LinkedHashMap<String, Pair<Object, Integer>>();
        this.labelMetaDataKey = null;
        this.tokenSequence = tokenSequence;
    }
    
    public Document(final String text) {
        this.tokenSequence = new ArrayList<Token>();
        this.metaData = new LinkedHashMap<String, Pair<Object, Integer>>();
        this.labelMetaDataKey = null;
        this.text = text;
        this.tokenSequence.add(new Token(text, 1.0f));
    }
    
    public Document(final List<Token> tokenSequence, final Document document) {
        this.tokenSequence = new ArrayList<Token>();
        this.metaData = new LinkedHashMap<String, Pair<Object, Integer>>();
        this.labelMetaDataKey = null;
        this.tokenSequence = tokenSequence;
        this.metaData.putAll(document.metaData);
    }
    
    public Document(final Document document) {
        this.tokenSequence = new ArrayList<Token>();
        this.metaData = new LinkedHashMap<String, Pair<Object, Integer>>();
        this.labelMetaDataKey = null;
        this.text = document.getDisplayText();
        this.tokenSequence = document.getTokenSequence();
        this.metaData = document.getMetaData();
        this.labelMetaDataKey = document.getLabelMetaDataKey();
    }
    
    @Deprecated
    public String getText() {
        return this.getDisplayText();
    }
    
    public String getTokenText() {
        if (this.tokenSequence.isEmpty()) {
            return "";
        }
        if (this.tokenSequence.size() == 1) {
            return String.valueOf(this.tokenSequence.get(0).getToken());
        }
        return this.getTokenString();
    }
    
    public String getDisplayText() {
        return this.getTokenString();
    }
    
    private String getTokenString() {
        if (this.text == null) {
            final StringBuilder buffer = new StringBuilder();
            for (final Token token : this.tokenSequence) {
                buffer.append(token.getToken());
                buffer.append(" ");
            }
            this.text = buffer.toString();
        }
        return this.text;
    }
    
    public List<Token> getTokenSequence() {
        return this.tokenSequence;
    }
    
    public Map<String, Pair<Object, Integer>> getMetaData() {
        return this.metaData;
    }
    
    public void setTokenSequence(final List<Token> tokenSequence) {
        this.tokenSequence = tokenSequence;
    }
    
    public Set<String> getMetaDataKeys() {
        return this.metaData.keySet();
    }
    
    public Object getMetaDataValue(final String metaDataKey) {
        final Pair<Object, Integer> pair = this.metaData.get(metaDataKey);
        if (pair != null) {
            return pair.getFirst();
        }
        return null;
    }
    
    public void addMetaData(final Map<String, Pair<Object, Integer>> metaData) {
        metaData.putAll(metaData);
    }
    
    public void addMetaData(final Document document) {
        this.metaData.putAll(document.metaData);
    }
    
    public void addMetaData(final String metaDataKey, final String value, final int valueType) {
        this.metaData.put(metaDataKey, (Pair<Object, Integer>)new Pair((Object)value, (Object)valueType));
    }
    
    public void addMetaData(final String metaDataKey, final double value, final int valueType) {
        this.metaData.put(metaDataKey, (Pair<Object, Integer>)new Pair((Object)value, (Object)valueType));
    }
    
    public void addMetaData(final String metaDataKey, final Date value, final int valueType) {
        this.metaData.put(metaDataKey, (Pair<Object, Integer>)new Pair((Object)value, (Object)valueType));
    }
    
    public int getMetaDataType(final String key) {
        final Pair<Object, Integer> pair = this.metaData.get(key);
        if (pair != null) {
            return (int)pair.getSecond();
        }
        return -1;
    }
    
    public IOObject copy() {
        return (IOObject)this.clone();
    }
    
    public Document clone() {
        try {
            final Class<? extends Document> clazz = this.getClass();
            final Constructor<? extends Document> cloneConstructor = clazz.getConstructor(clazz);
            final Document result = (Document)cloneConstructor.newInstance(this);
            return result;
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException("Cannot clone Document: " + e.getMessage());
        }
        catch (NoSuchMethodException e4) {
            throw new RuntimeException("'" + this.getClass().getName() + "' does not implement clone constructor!");
        }
        catch (InvocationTargetException e2) {
            throw new RuntimeException("Cannot clone " + this.getClass().getName() + ": " + e2 + ". Target: " + e2.getTargetException() + ". Cause: " + e2.getCause() + ".");
        }
        catch (InstantiationException e3) {
            throw new RuntimeException("Cannot clone " + this.getClass().getName() + ": " + e3);
        }
    }
    
    public boolean isLabeled() {
        return this.labelMetaDataKey != null;
    }
    
    public String getLabelMetaDataKey() {
        return this.labelMetaDataKey;
    }
    
    public int getLabelType() {
        final Pair<Object, Integer> metaDataPair = this.metaData.get(this.labelMetaDataKey);
        if (metaDataPair != null) {
            return (int)metaDataPair.getSecond();
        }
        return 0;
    }
    
    public String getNominalLabel() {
        final Pair<Object, Integer> metaDataPair = this.metaData.get(this.labelMetaDataKey);
        if (metaDataPair != null) {
            return (String)metaDataPair.getFirst();
        }
        return "?";
    }
    
    public double getNumericalLabel() {
        final Pair<Object, Integer> metaDataPair = this.metaData.get(this.labelMetaDataKey);
        if (metaDataPair != null) {
            return (double)metaDataPair.getFirst();
        }
        return Double.NaN;
    }
    
    public String getExtension() {
        return "txt";
    }
    
    public String getFileDescription() {
        return "Text";
    }
    
    public void setLabel(final String metaDataKey, final String value, final int valueType) {
        this.addMetaData(this.labelMetaDataKey = metaDataKey, value, valueType);
    }
    
    public void setLabel(final String metaDataKey, final double value, final int valueType) {
        this.addMetaData(this.labelMetaDataKey = metaDataKey, value, valueType);
    }
    
    public String toString() {
        final StringBuffer buffer = new StringBuffer();
        buffer.append("Text consists of " + this.tokenSequence.size() + " tokens");
        buffer.append(Tools.getLineSeparator());
        buffer.append(this.getDisplayText());
        return buffer.toString();
    }
    
    public String toResultString() {
        return this.getDisplayText();
    }
}