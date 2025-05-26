package com.morpheus.test

import spock.lang.Specification





class MyTestSpec extends Specification {

    def "should say hello"() {
        expect:
        "hello".toUpperCase() == "HELLO"
    }

}
