package com.github.kladdkaka.gtfs.filter;

@FunctionalInterface
public interface CheckedBiConsumer<T, U, E extends Exception> {

    /**
     * Performs this operation on the given arguments.
     *
     * @param t the first input argument
     * @param u the second input argument
     */
    void accept(T t, U u) throws E;

}
