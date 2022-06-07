package com.luban.kafkastream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.nio.charset.StandardCharsets;

public class LogProcessor implements Processor<byte[], byte[]> {
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    @Override
    public void process(byte[] dummy, byte[] line) {
        String input = new String(line);
        if (input.contains("MOVIE_RATING_PREFIX:")) {
            System.out.println("movie rating data coming.... " + input);
            input = input.split("MOVIE_RATING_PREFIX:")[1].trim();
            context.forward("logProcessor".getBytes(StandardCharsets.UTF_8), input.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
