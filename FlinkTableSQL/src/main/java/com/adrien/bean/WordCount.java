package com.adrien.bean;

public class WordCount {
    public String word;
    public long frequency;

    public WordCount() {
    }
    public WordCount(String word, long frequency) {
        this.word = word;
        this.frequency = frequency;
    }

    @Override
    public String toString() {
        return "WordCount{" +
                "word='" + word + '\'' +
                ", frequency=" + frequency +
                '}';
    }

}
