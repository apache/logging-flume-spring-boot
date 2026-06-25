/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.spring.boot.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.Sink;
import org.apache.flume.SinkProcessor;
import org.apache.flume.SinkRunner;
import org.apache.flume.Source;
import org.apache.flume.SourceRunner;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.conf.channel.ChannelType;
import org.apache.flume.interceptor.Interceptor;

/**
 * The primarily provides helper methods to create and configure the various Flume components.
 * The do not have to be used.
 */
public abstract class AbstractFlumeConfiguration {

    /**
     * Create amd configure a Channel.
     * @param name the Channel name.
     * @param clazz the Channel Class to create.
     * @param params parameters needed for configuration.
     * @return The Channel.
     * @param <T> The specific type of Channel.
     */
    protected <T extends Channel> T configureChannel(
            final String name, final Class<T> clazz, final Map<String, String> params) {
        T channel;
        try {
            channel = clazz.getDeclaredConstructor().newInstance();
        } catch (Exception ex) {
            throw new FlumeException("Unable to create channel " + name, ex);
        }
        channel.setName(name);
        Configurables.configure(channel, createContext(params));
        return channel;
    }

    /**
     * Create and configure a Source and its SourcRunner. A ChannelSelector may be provided.
     * @param name The name of the Source.
     * @param clazz The Source class to be created and configured.
     * @param selector The Channel Selector.
     * @param params parameters required for configuration.
     * @return The SourceRunner.
     * @param <T> The Source Class.
     */
    protected <T extends Source> SourceRunner configureSource(
            final String name, final Class<T> clazz, final ChannelSelector selector, final Map<String, String> params) {
        return configureSource(name, clazz, selector, null, params);
    }

    /**
     * Create and configure a Source and its SourcRunner. A ChannelSelector and Interceptors may be provided.
     * @param name The name of the Source.
     * @param clazz The Source class to be created and configured.
     * @param selector The Channel Selector.
     * @param interceptors A List of Interceptors.
     * @param params parameters required for configuration.
     * @return The SourceRunner.
     * @param <T> The Source Class.
     */
    protected <T extends Source> SourceRunner configureSource(
            final String name,
            final Class<T> clazz,
            final ChannelSelector selector,
            final List<Interceptor> interceptors,
            final Map<String, String> params) {
        T source;
        try {
            source = clazz.getDeclaredConstructor().newInstance();
        } catch (Exception ex) {
            throw new FlumeException("Unable to create source " + name, ex);
        }
        source.setName(name);
        ChannelSelector channelSelector = selector != null ? selector : new ReplicatingChannelSelector();
        Configurables.configure(source, createContext(params));
        return configureSource(source, channelSelector);
    }

    /**
     * Set up an already configured Source for processing.
     * @param source The Source.
     * @param selector The Channel Selector.
     * @return The SourceRunner.
     * @param <T> The Source Class.
     */
    protected <T extends Source> SourceRunner configureSource(final T source, final ChannelSelector selector) {
        ChannelSelector channelSelector = selector != null ? selector : new ReplicatingChannelSelector();
        source.setChannelProcessor(new ChannelProcessor(channelSelector));
        return SourceRunner.forSource(source);
    }

    /**
     * Set up already configured Sinks for processing.
     * @param params The SinkProcessor's parameters.
     * @param clazz The SinkProcessor Class.
     * @param sinks The list of Sinks.
     * @return The SinkProcessor.
     * @param <T> The specific type of the SinkProcessor.
     */
    protected <T extends SinkProcessor> T configureSinkProcessor(
            final Map<String, String> params, final Class<T> clazz, final List<Sink> sinks) {
        T processor;
        try {
            processor = clazz.getDeclaredConstructor().newInstance();
        } catch (Exception ex) {
            throw new FlumeException("Unable to create SinkProcessor of type: " + clazz.getName(), ex);
        }
        processor.setSinks(sinks);
        Configurables.configure(processor, createContext(params));
        return processor;
    }

    /**
     * Set up the SinkProcessors and SinkRunners for a list of Sinks.
     * @param processorProperties The SinkProcessor's parameters.
     * @param sinkProcessorClass The SinkProcessor to create.
     * @param sinks The Map of Sink Lists with the key being the name of a group of channels to which the sinks are attached.
     * @return A Map of the SinkRunners
     */
    protected Map<String, SinkRunner> createSinkRunners(
            final Map<String, String> processorProperties,
            final Class<? extends SinkProcessor> sinkProcessorClass,
            final Map<String, List<Sink>> sinks) {
        Map<String, SinkRunner> sinkRunners = new HashMap<>();
        for (Map.Entry<String, List<Sink>> entry : sinks.entrySet()) {
            sinkRunners.put(
                    entry.getKey(),
                    createSinkRunner(
                            configureSinkProcessor(processorProperties, sinkProcessorClass, entry.getValue())));
        }
        return sinkRunners;
    }

    /**
     * Creates the SinkRunner.
     * @param sinkProcessor The SinkProcessor.
     * @return A SinkRunner.
     */
    protected SinkRunner createSinkRunner(SinkProcessor sinkProcessor) {
        SinkRunner runner = new SinkRunner(sinkProcessor);
        runner.setSink(sinkProcessor);
        return runner;
    }

    /**
     * Create and configure a Sink.
     * @param name The name of the Sink.
     * @param sinkClazz The Class object for the Sink.
     * @param channel The Channel attached to the Sink.
     * @param params Parameters to configure the Sink.
     * @return The Sink.
     * @param <T> The type of Sink being created.
     */
    protected <T extends Sink> Sink configureSink(
            final String name, final Class<T> sinkClazz, final Channel channel, final Map<String, String> params) {
        T sink;
        try {
            sink = sinkClazz.getDeclaredConstructor().newInstance();
        } catch (Exception ex) {
            throw new FlumeException("Unable to create sink " + name, ex);
        }
        sink.setName(name);
        Configurables.configure(sink, createContext(params));
        sink.setChannel(channel);
        return sink;
    }

    /**
     * Create the channel selector and configure it.
     * @param clazz    The Selector class.
     * @param channels The Channels.
     * @param params   The configuration parameters.
     * @return The ChannelSelector.
     */
    protected ChannelSelector createChannelSelector(
            Class<? extends ChannelSelector> clazz, List<Channel> channels, Map<String, String> params) {
        ChannelSelector selector;
        try {
            selector = clazz.getDeclaredConstructor().newInstance();
        } catch (Exception ex) {
            throw new FlumeException("Unable to create channel selector " + clazz.getName(), ex);
        }
        selector.setChannels(channels);
        Configurables.configure(selector, createContext(params));
        return selector;
    }

    @SuppressWarnings("unchecked")
    protected Class<? extends Channel> getChannelClass(String type) {
        if (type == null) {
            return null;
        } else {
            ChannelType channelType = null;
            try {
                channelType = ChannelType.valueOf(type.toUpperCase(Locale.getDefault()));
                return Class.forName(channelType.name()).asSubclass(Channel.class);
            } catch (Exception ex) {
                return null;
            }
        }
    }

    /**
     * Creates a List from a Varargs array.
     *
     * @param items The items to add to the list.
     * @param <T>   The type of objects in the List.
     * @return a List containing the supplied items.
     */
    @SafeVarargs
    protected final <T> List<T> listOf(T... items) {
        return Arrays.asList(items);
    }

    /**
     * Create a Context from the Map.
     *
     * @param map contains the configuration parameters for the component being provisioned.
     * @return The Context.
     */
    protected static Context createContext(Map<String, String> map) {
        return map != null ? new Context(map) : new Context();
    }
}
