package com.github.highcumontoa.quorumlogreplicationjava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author Vincent
 * @version 1.0
 */
@SpringBootTest(properties = "raft.data-dir=target/it-data-context")
public class ApplicationTests {
    @Test
    public void contentLoad() {
    }
}
