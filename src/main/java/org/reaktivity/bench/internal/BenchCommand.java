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
package org.reaktivity.bench.internal;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static org.agrona.IoUtil.tmpDirName;
import static org.reaktivity.reaktor.internal.ReaktorConfiguration.REAKTOR_DIRECTORY;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.agrona.LangUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.reaktor.Reaktor;
import org.reaktivity.reaktor.internal.ReaktorConfiguration;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public final class BenchCommand
{
    public static void main(
        String[] args) throws Exception
    {
        CommandLineParser parser = new DefaultParser();

        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("help").desc("print this message").build());
        options.addOption(Option.builder("d").longOpt("directory").hasArg().desc("configuration directory").build());
        options.addOption(Option.builder("t").longOpt("threads").hasArg().desc("thread count").build());
        options.addOption(Option.builder("a").longOpt("affinity").hasArgs().desc("thread affinity mask").build());
        options.addOption(Option.builder("p").longOpt("profile").hasArgs().desc("profile filename").build());
        options.addOption(Option.builder().longOpt("duration").hasArgs().desc("duration (secs)").build());
        options.addOption(Option.builder().longOpt("warmups").hasArgs().desc("warmups (iterations)").build());
        options.addOption(Option.builder().longOpt("debug").desc("debug").build());

        CommandLine cmdline = parser.parse(options, args);

        if (cmdline.hasOption("help"))
        {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("reaktor", options);
        }
        else
        {
            new BenchCommand(cmdline).execute();
        }
    }

    private final CommandLine cmdline;
    private final Gson gson;

    private BenchCommand(
        CommandLine cmdline)
    {
        this.cmdline = cmdline;
        gson = new Gson();
    }

    private void execute()
    {
        final String profile = cmdline.getOptionValue("profile");
        final int warmups = parseInt(cmdline.getOptionValue("warmups", "0"));

        try (Reader reader = Files.newBufferedReader(Paths.get(profile)))
        {
            final JsonParser json = new JsonParser();
            final JsonObject object = json.parse(reader).getAsJsonObject();
            final JsonArray routes = object.getAsJsonArray("routes");

            final String address = gson.fromJson(object.get("address"), String.class);
            final JsonElement extension = object.get("extension");
            final DriverParameters parameters = gson.fromJson(extension, DriverParameters.class);
            final int connections = parameters.connections;
            final int senders = parameters.senders;
            final int messages = parameters.messages;
            final int size = parameters.size;
            final Benchmark benchmark = new Benchmark(address, connections, senders, messages, size, warmups);

            execute(benchmark, routes);

            System.out.format("%d,%d,%f\n", connections, senders, benchmark.results());
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void execute(
        final Benchmark benchmark,
        final JsonArray routes) throws Exception
    {
        final String directory = cmdline.getOptionValue("directory", format("%s/bench.ry", tmpDirName()));
        final String[] affinities = cmdline.getOptionValues("affinity");
        final int threads = Integer.parseInt(cmdline.getOptionValue("threads", "1"));
        final int duration = Integer.parseInt(cmdline.getOptionValue("duration", "1"));
        final boolean debug = cmdline.hasOption("debug");

        Properties defaultOverrides = new Properties();
        defaultOverrides.setProperty(REAKTOR_DIRECTORY.name(), directory);
        ReaktorConfiguration config = new ReaktorConfiguration(new Configuration(), defaultOverrides);

        try (Reaktor reaktor = Reaktor.builder()
                .config(config)
                .threads(threads)
                .affinityMaskDefault(affinityMask(threads, affinities))
                .nukleus(n -> true)
                .controller(c -> true)
                .supplyAgentBuilder(benchmark::supplyAgentBuilder)
                .errorHandler(ex -> ex.printStackTrace(System.err))
                .build()
                .start())
        {
            if (debug)
            {
                printConfig(reaktor, System.out::printf);
            }

            for (JsonElement route : routes)
            {
                JsonObject routeObject = route.getAsJsonObject();
                String nukleus = gson.fromJson(routeObject.get("nukleus"), String.class);
                RouteKind kind = gson.fromJson(routeObject.get("role"), RouteKind.class);
                String local = gson.fromJson(routeObject.get("local"), String.class);
                String remote = gson.fromJson(routeObject.get("remote"), String.class);
                String extension = gson.toJson(routeObject.get("extension"));

                Controller controller = reaktor.controllers()
                                               .filter(c -> nukleus.equals(c.name()))
                                               .findFirst()
                                               .orElse(null);
                assert controller != null;
                controller.route(kind, local, remote, extension).get();
            }

            benchmark.start();
            Thread.sleep(TimeUnit.SECONDS.toMillis(duration));
            benchmark.stop();
        }
    }

    private static Function<String, BitSet> affinityMask(
        final int threads,
        final String[] affinities)
    {
        Map<Pattern, BitSet> affinityMasks = new LinkedHashMap<>();
        if (affinities != null)
        {
            for (String affinity : affinities)
            {
                final String[] parts = affinity.split(":");
                final String regex = parts[0].contains("#") ? parts[0] : String.format("%s#.+", parts[0]);
                final Pattern addressMatch = Pattern.compile(regex);
                final long affinityMask = Long.decode(parts[1]);
                final BitSet affinityBits = BitSet.valueOf(new long[] { affinityMask });
                affinityMasks.put(addressMatch, affinityBits);
            }
        }

        BitSet affinityMaskDefault = BitSet.valueOf(new long[] { (1L << threads) - 1L });
        Function<String, BitSet> affinityMask = a -> affinityMasks.entrySet()
                                                                  .stream()
                                                                  .filter(e -> e.getKey().matcher(a).matches())
                                                                  .map(Map.Entry::getValue)
                                                                  .findFirst()
                                                                  .orElse(affinityMaskDefault);
        return affinityMask;
    }

    private static void printConfig(
        Reaktor reaktor,
        Consumer<String> out) throws IOException
    {
        final Set<String> properties = new TreeSet<>();
        reaktor.properties((s, o) -> properties.add(String.format("%s = %s\n", s, o)),
                           (s, o) -> properties.add(String.format("%s = %s (default)\n", s, o)));
        properties.forEach(out);
    }

    private static final class DriverParameters
    {
        public int connections;
        public int senders;
        public int messages;
        public int size;
    }
}
