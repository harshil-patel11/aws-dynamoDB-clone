package com.g10.CPEN431.A11;

public class Pair<T, U> {
    private final T returnVal;
    private final U version;

    public Pair(T first, U second) {
        this.returnVal = first;
        this.version = second;
    }

    public T getFirstVal() {
        return returnVal;
    }

    public U getSecondVal() {
        return version;
    }
}
