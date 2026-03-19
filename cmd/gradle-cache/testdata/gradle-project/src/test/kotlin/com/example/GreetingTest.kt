package com.example

import kotlin.test.Test
import kotlin.test.assertEquals

class GreetingTest {
    @Test
    fun testHello() {
        assertEquals("Hello, World!", Greeting().hello("World"))
    }
}
