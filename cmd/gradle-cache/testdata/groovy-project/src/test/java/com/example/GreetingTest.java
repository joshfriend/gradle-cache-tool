package com.example;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class GreetingTest {
    @Test
    public void testGreet() {
        assertEquals("Hello, World!", new Greeting().greet("World"));
    }
}
