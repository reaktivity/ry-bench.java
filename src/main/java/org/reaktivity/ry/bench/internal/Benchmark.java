/**
 * Copyright 2016-2019 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.ry.bench.internal;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toCollection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.ry.bench.internal.types.OctetsFW;
import org.reaktivity.ry.bench.internal.types.stream.AbortFW;
import org.reaktivity.ry.bench.internal.types.stream.BeginFW;
import org.reaktivity.ry.bench.internal.types.stream.DataFW;
import org.reaktivity.ry.bench.internal.types.stream.EndFW;
import org.reaktivity.ry.bench.internal.types.stream.FrameFW;
import org.reaktivity.ry.bench.internal.types.stream.ResetFW;
import org.reaktivity.ry.bench.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.AgentBuilder;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.reaktor.internal.router.RouteId;
import org.reaktivity.reaktor.internal.types.control.Role;

public class Benchmark
{
    private final String address;
    private final int connections;
    private final int senders;
    private final int messages;
    private final int warmups;
    private final OctetsFW payload;
    private final List<BenchmarkAgent> agents;
    private final Consumer<BenchmarkAgent> iterateTask;
    private final Consumer<BenchmarkAgent> stopTask;

    private volatile List<BenchmarkAgent> activeAgents;
    private volatile long startAt;
    private volatile Semaphore stopped;
    private int iterations;

    public Benchmark(
        String address,
        int connections,
        int senders,
        int messages,
        int size,
        int warmups)
    {
        this.address = address;
        this.connections = connections;
        this.senders = senders;
        this.messages = messages;
        this.warmups = warmups;
        this.payload = initPayload(size);
        this.agents = new CopyOnWriteArrayList<>();

        this.iterateTask = a -> a.submit(BenchmarkAgent::iterate);
        this.stopTask = a -> a.submit(BenchmarkAgent::stop);
    }

    public void start()
    {
        this.activeAgents = agents.stream()
                                  .filter(BenchmarkAgent::activate)
                                  .collect(toCollection(ArrayList::new));

        final int buckets = activeAgents.size();

        final Semaphore started = new Semaphore(connections);
        final Semaphore stopped = new Semaphore(connections);
        final Supplier<Semaphore> supplySent = new RepeatingSupplier<>(() -> new Semaphore(senders), buckets);
        final Supplier<Semaphore> supplyReceived = new RepeatingSupplier<>(() -> new Semaphore(messages - 1), buckets);
        final Runnable reiterate = this::iterate;

        acquire(started, connections);

        int bucket = 0;
        for (BenchmarkAgent activeAgent : activeAgents)
        {
            final int bucketSize = Math.round((bucket + 1) * connections / (float) buckets) -
                                    Math.round(bucket * connections / (float) buckets);

            final Consumer<BenchmarkAgent> startTask = a ->
                a.start(bucketSize, started, stopped, supplySent, supplyReceived, reiterate);
            activeAgent.submit(startTask);

            bucket++;
        }

        acquire(started, connections);

        System.out.format("[%s] iterating\n", Thread.currentThread());

        this.startAt = System.currentTimeMillis();
        this.stopped = stopped;
        this.iterations = -warmups;

        activeAgents.forEach(iterateTask);
    }

    private void iterate()
    {
        if (iterations == 0L)
        {
            this.startAt = System.currentTimeMillis();
        }

//        System.out.format("[%s] iterate() %d\n", Thread.currentThread(), iterations);
        iterations++;
        activeAgents.forEach(iterateTask);
    }

    public void stop()
    {
        acquire(stopped, connections);
        activeAgents.forEach(stopTask);
        acquire(stopped, connections);
    }

    public double results()
    {
        long frames = 0L;
        long messages = 0L;
        float duration = 0;
        Histogram histogram = new Histogram(1024, 3);

        for (BenchmarkAgent activeAgent : activeAgents)
        {
            frames += activeAgent.frames;
            messages += activeAgent.messages;
            duration = Math.max(duration, activeAgent.completedAt - startAt);
            histogram.add(activeAgent.histogram);
        }

        histogram.outputPercentileDistribution(System.out, 1.0);
        System.out.format("%d %d %d %d\n", messages, frames, iterations, (int) duration);

        return duration / iterations;
    }

    public AgentBuilder supplyAgentBuilder()
    {
        return new BenchmarkAgentBuilder();
    }

    private final class BenchmarkAgentBuilder implements AgentBuilder
    {
        private ToIntFunction<String> supplyAddressId;
        private IntFunction<StreamFactory> supplyStreamFactory;
        private LongFunction<MessageConsumer> supplyThrottle;
        private LongConsumer removeThrottle;
        private LongUnaryOperator supplyInitialId;
        private LongUnaryOperator supplyReplyId;
        private LongSupplier supplyTraceId;
        private MutableDirectBuffer writeBuffer;

        @Override
        public AgentBuilder setAddressIdSupplier(
            ToIntFunction<String> supplyAddressId)
        {
            this.supplyAddressId = supplyAddressId;
            return this;
        }

        @Override
        public AgentBuilder setStreamFactorySupplier(
            IntFunction<StreamFactory> supplyStreamFactory)
        {
            this.supplyStreamFactory = supplyStreamFactory;
            return this;
        }

        @Override
        public AgentBuilder setThrottleSupplier(
            LongFunction<MessageConsumer> supplyThrottle)
        {
            this.supplyThrottle = supplyThrottle;
            return this;
        }

        @Override
        public AgentBuilder setThrottleRemover(
            LongConsumer removeThrottle)
        {
            this.removeThrottle = removeThrottle;
            return this;
        }

        @Override
        public AgentBuilder setInitialIdSupplier(
            LongUnaryOperator supplyInitialId)
        {
            this.supplyInitialId = supplyInitialId;
            return this;
        }

        @Override
        public AgentBuilder setReplyIdSupplier(
            LongUnaryOperator supplyReplyId)
        {
            this.supplyReplyId = supplyReplyId;
            return this;
        }

        @Override
        public AgentBuilder setTraceIdSupplier(
            LongSupplier supplyTraceId)
        {
            this.supplyTraceId = supplyTraceId;
            return this;
        }

        @Override
        public AgentBuilder setWriteBuffer(
            MutableDirectBuffer writeBuffer)
        {
            this.writeBuffer = writeBuffer;
            return this;
        }

        @Override
        public Agent build()
        {
            final BenchmarkAgent agent = new BenchmarkAgent(supplyAddressId, supplyStreamFactory, supplyThrottle, removeThrottle,
                                                            supplyInitialId, supplyReplyId, supplyTraceId, writeBuffer);
            agents.add(agent);
            return agent;
        }
    }

    private final class BenchmarkAgent implements Agent
    {
        private final FrameFW frameRO = new FrameFW();
        private final BeginFW beginRO = new BeginFW();
        private final DataFW dataRO = new DataFW();
        private final EndFW endRO = new EndFW();
        private final WindowFW windowRO = new WindowFW();

        private final BeginFW.Builder beginRW = new BeginFW.Builder();
        private final DataFW.Builder dataRW = new DataFW.Builder();
        private final EndFW.Builder endRW = new EndFW.Builder();

        private final WindowFW.Builder windowRW = new WindowFW.Builder();

        private final ToIntFunction<String> supplyAddressId;
        private final IntFunction<StreamFactory> supplyStreamFactory;
        private final LongFunction<MessageConsumer> supplyThrottle;
        private final LongConsumer removeThrottle;
        private final LongUnaryOperator supplyInitialId;
        private final LongUnaryOperator supplyReplyId;
        private final LongSupplier supplyTraceId;
        private final MutableDirectBuffer writeBuffer;
        private final Deque<Consumer<BenchmarkAgent>> taskQueue;
        private final Long2ObjectHashMap<MessageConsumer> streamsById;
        private final Histogram histogram;

        private volatile int addressId;
        private volatile long routeId;
        private volatile BenchmarkStream[] streams;
        private volatile int startsPending;
        private volatile StreamFactory streamFactory;

        private volatile Semaphore started;
        private volatile Semaphore received;
        private volatile Semaphore stopped;
        private volatile Supplier<Semaphore> supplySent;
        private volatile Supplier<Semaphore> supplyReceived;
        private volatile Runnable reiterate;

        private volatile long completedAt;
        private volatile long frames;
        private volatile long messages;
        private volatile int starting;
        private volatile long startedAt;
        private volatile int stopping;

        private BenchmarkAgent(
            ToIntFunction<String> supplyAddressId,
            IntFunction<StreamFactory> supplyStreamFactory,
            LongFunction<MessageConsumer> supplyThrottle,
            LongConsumer removeThrottle,
            LongUnaryOperator supplyInitialId,
            LongUnaryOperator supplyReplyId,
            LongSupplier supplyTraceId,
            MutableDirectBuffer writeBuffer)
        {
            this.supplyAddressId = supplyAddressId;
            this.supplyStreamFactory = supplyStreamFactory;
            this.supplyThrottle = supplyThrottle;
            this.removeThrottle = removeThrottle;
            this.supplyInitialId = supplyInitialId;
            this.supplyReplyId = supplyReplyId;
            this.supplyTraceId = supplyTraceId;
            this.writeBuffer = writeBuffer;
            this.streamsById = new Long2ObjectHashMap<>();
            this.taskQueue = new ConcurrentLinkedDeque<>();
            this.histogram = new Histogram(SECONDS.toMillis(1), 3);
        }

        @Override
        public int doWork() throws Exception
        {
            int tasksProcessed = 0;

            for (Consumer<BenchmarkAgent> task = taskQueue.pollFirst();
                    task != null;
                    task = taskQueue.pollFirst())
            {
                task.accept(this);
                tasksProcessed++;
            }

            return tasksProcessed;
        }

        @Override
        public String roleName()
        {
            return "benchmark";
        }

        private boolean submit(
            Consumer<BenchmarkAgent> task)
        {
            return taskQueue.offer(task);
        }

        private void start(
            int count,
            Semaphore started,
            Semaphore stopped,
            Supplier<Semaphore> supplySent,
            Supplier<Semaphore> supplyReceived,
            Runnable reiterate)
        {
            this.started = started;
            this.stopped = stopped;
            this.supplySent = supplySent;
            this.supplyReceived = supplyReceived;
            this.reiterate = reiterate;

            assert streamFactory != null;

            // TODO: need supplier to generate routeId from addressId
            this.routeId = RouteId.routeId(255, addressId, Role.CLIENT, 0);
            this.streams = new BenchmarkStream[count];
            this.starting = 0;
            this.stopping = 0;

            System.out.format("[%s] starting %d streams\n", Thread.currentThread(), streams.length);

            startBatch();
        }

        private void iterate()
        {
            final long now = System.currentTimeMillis();
            if (iterations >= 0)
            {
                final long millis = now - startedAt;
                histogram.recordValue(millis);
//                System.out.format("[%d] %d\n", iterations, (int) millis);
            }
            this.completedAt = now;

            if (streamFactory != null)
            {
                this.startedAt = System.currentTimeMillis();
                this.received = supplyReceived.get();

                final Random random = ThreadLocalRandom.current();
                final Semaphore sent = supplySent.get();
                while (sent.tryAcquire())
                {
                    streams[random.nextInt(streams.length)].doData(payload);
                }
            }
            else if (stopping > 0)
            {
                for (BenchmarkStream stream : streams)
                {
                    stream.doEnd();
                }

                System.out.format("[%s] stopped %d streams\n", Thread.currentThread(), stopping);

                stopping = 0;
            }
        }

        private void stop()
        {
            assert streamFactory != null;

            stopping = streams.length;

            System.out.format("[%s] stopping %d streams\n", Thread.currentThread(), stopping);

            streamFactory = null;
        }

        private boolean activate()
        {
            this.addressId = supplyAddressId.applyAsInt(address);
            this.streamFactory = supplyStreamFactory.apply(addressId);
            return this.streamFactory != null;
        }

        private void startBatch()
        {
            final int maxStartsPending = 1;
            assert startsPending == 0;

            final MessageConsumer readHandler = this::handleRead;
            for (int i=0; i < maxStartsPending && starting < streams.length; i++)
            {
                final long initialId = supplyInitialId.applyAsLong(routeId);
                final long replyId = supplyReplyId.applyAsLong(initialId);
                final long traceId = supplyTraceId.getAsLong();
                final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                             .routeId(routeId)
                                             .streamId(initialId)
                                             .trace(traceId)
                                             .build();

                final BenchmarkStream newStream = new BenchmarkStream(routeId, initialId, replyId);

                startsPending++;
                streams[starting++] = newStream;
                streamsById.put(initialId, newStream::onMessage);
                streamsById.put(replyId, newStream::onMessage);

                newStream.init(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof(), readHandler);
            }
        }

        private void handleRead(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            final FrameFW frame = frameRO.wrap(buffer, index, index + length);
            final long streamId = frame.streamId();

            final MessageConsumer stream = streamsById.get(streamId);
            if (stream != null)
            {
                stream.accept(msgTypeId, buffer, index, length);

                switch (msgTypeId)
                {
                case ResetFW.TYPE_ID:
                    streamsById.remove(streamId);
                    break;
                case AbortFW.TYPE_ID:
                case EndFW.TYPE_ID:
                    removeThrottle.accept(streamId);
                    streamsById.remove(streamId);
                    break;
                default:
                    break;
                }
            }
            else
            {
                System.out.println("not handled");
            }
        }

        private final class BenchmarkStream
        {
            private final long routeId;
            private final long initialId;
            private final long replyId;
            private MessageConsumer receiver;

            private boolean readable;
            private boolean writable;

            private BenchmarkStream(
                long routeId,
                long initialId,
                long replyId)
            {
                this.routeId = routeId;
                this.initialId = initialId;
                this.replyId = replyId;
            }

            public void init(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length,
                MessageConsumer sender)
            {
                final MessageConsumer receiver = streamFactory.newStream(msgTypeId, buffer, index, length, sender);
                receiver.accept(msgTypeId, buffer, index, length);
                this.receiver = receiver;
            }

            public void doData(
                OctetsFW payload)
            {
                final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                          .routeId(routeId)
                                          .streamId(initialId)
                                          .trace(supplyTraceId.getAsLong())
                                          .groupId(0L)
                                          .padding(0)
                                          .payload(payload)
                                          .build();

                receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
            }

            public void doEnd()
            {
                final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                       .routeId(routeId)
                                       .streamId(initialId)
                                       .build();

                receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
            }

            private void doWindow(
                int credit)
            {
                final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                                .routeId(routeId)
                                                .streamId(replyId)
                                                .credit(credit)
                                                .padding(0)
                                                .groupId(0L)
                                                .build();

                supplyThrottle.apply(replyId).accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
            }

            private void onBegin(
                BeginFW begin)
            {
                doWindow(1 << 30);

                if (!readable)
                {
                    readable = true;

                    if (readable && writable)
                    {
                        started.release();
                    }
                }

                if (--startsPending == 0)
                {
                    startBatch();
                }
            }

            private void onData(
                DataFW data)
            {
                final int count = data.length() / payload.sizeof();
                frames++;
                messages += count;

                final int credit = data.length() + data.padding();
                doWindow(credit);

                if (!received.tryAcquire(count))
                {
                    reiterate.run();
                }
            }

            private void onEnd(
                EndFW end)
            {
                stopped.release();
            }

            private void onWindow(
                WindowFW window)
            {
                if (!writable)
                {
                    writable = true;

                    if (readable && writable)
                    {
                        started.release();
                    }
                }
            }

            private void onMessage(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
            {
                switch (msgTypeId)
                {
                case BeginFW.TYPE_ID:
                    final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                    onBegin(begin);
                    break;
                case DataFW.TYPE_ID:
                    final DataFW data = dataRO.wrap(buffer, index, index + length);
                    onData(data);
                    break;
                case EndFW.TYPE_ID:
                    final EndFW end = endRO.wrap(buffer, index, index + length);
                    onEnd(end);
                    break;
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    onWindow(window);
                    break;
                }
            }
        }
    }

    private static void acquire(
        final Semaphore semaphore,
        final int permits)
    {
        try
        {
            semaphore.acquire(permits);
        }
        catch (InterruptedException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private static final class RepeatingSupplier<T> implements Supplier<T>
    {
        private final Supplier<T> supplier;
        private final int repeats;

        private int index;
        private T cache;

        private RepeatingSupplier(
            Supplier<T> supplier,
            int repeats)
        {
            this.supplier = supplier;
            this.repeats = repeats;
        }

        @Override
        public synchronized T get()
        {
            if (index == 0)
            {
                cache = supplier.get();
            }
            index = (index + 1) % repeats;
            return cache;
        }
    }

    private static OctetsFW initPayload(
        int size)
    {
        final byte[] array = new byte[size];
        Arrays.fill(array, (byte) 120); // ASCII 'x'
        return new OctetsFW().wrap(new UnsafeBuffer(array), 0, array.length);
    }
}
