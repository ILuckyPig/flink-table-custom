package com.lu.table.connector.clickhouse.partitioner.factories;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class PartitionerFactoryService {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionerFactoryService.class);

    /**
     * Finds a table factory of the given class and property map.
     *
     * @param factoryClass desired factory class
     * @param shardingKey  properties that describe the factory configuration
     * @param <T>          factory class type
     * @return the matching factory
     */
    public static <T extends PartitionerFactory> T find(Class<T> factoryClass, String shardingKey) {
        return findSingleInternal(factoryClass, shardingKey, Optional.empty());
    }

    public static <T> List<PartitionerFactory> find(Class<T> factoryClass) {
        List<PartitionerFactory> partitionerFactories = discoverFactories(Optional.empty());
        return filterByFactoryClass(factoryClass, partitionerFactories);
    }

    /**
     * Finds a table factory of the given class, property map, and classloader.
     *
     * @param factoryClass desired factory class
     * @param shardingKey  properties that describe the factory configuration
     * @param classLoader  classloader for service loading
     * @param <T>          factory class type
     * @return the matching factory
     */
    private static <T extends PartitionerFactory> T findSingleInternal(Class<T> factoryClass, String shardingKey,
                                                                       Optional<ClassLoader> classLoader) {

        List<PartitionerFactory> partitionerFactories = discoverFactories(classLoader);
        List<T> filtered = filter(partitionerFactories, factoryClass, shardingKey);

        if (filtered.size() > 1) {
            throw new RuntimeException(
                    filtered +
                            factoryClass.toString() +
                            partitionerFactories +
                            shardingKey);
        } else {
            return filtered.get(0);
        }
    }

    /**
     * Searches for factories using Java service providers.
     *
     * @return all factories in the classpath
     */
    private static List<PartitionerFactory> discoverFactories(Optional<ClassLoader> classLoader) {
        try {
            List<PartitionerFactory> result = new LinkedList<>();
            ClassLoader cl = classLoader.orElse(Thread.currentThread().getContextClassLoader());
            ServiceLoader
                    .load(PartitionerFactory.class, cl)
                    .iterator()
                    .forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            LOG.error("Could not load service provider for table factories.", e);
            throw new TableException("Could not load service provider for table factories.", e);
        }
    }

    /**
     * Filters found factories by factory class and with matching context.
     */
    private static <T extends PartitionerFactory> List<T> filter(List<PartitionerFactory> foundFactories,
                                                                 Class<T> factoryClass, String shardingKey) {

        Preconditions.checkNotNull(factoryClass);
        Preconditions.checkNotNull(shardingKey);

        List<T> classFactories = filterByFactoryClass(factoryClass, shardingKey, foundFactories);

        return filterByFactoryIdentifier(factoryClass, shardingKey, classFactories);
    }

    /**
     * Filters factories with matching context by factory class.
     */
    @SuppressWarnings("unchecked")
    private static <T> List<T> filterByFactoryClass(Class<T> factoryClass, String shardingKey,
                                                    List<PartitionerFactory> foundFactories) {

        List<PartitionerFactory> classFactories = filterByFactoryClass(factoryClass, foundFactories);

        if (classFactories.isEmpty()) {
            throw new RuntimeException(
                    String.format("No factory implements '%s'.", factoryClass.getCanonicalName()) +
                            factoryClass +
                            foundFactories +
                            shardingKey);
        }

        return (List<T>) classFactories;
    }

    private static <T> List<PartitionerFactory> filterByFactoryClass(Class<T> factoryClass,
                                                                     List<PartitionerFactory> foundFactories) {
        return foundFactories.stream()
                .filter(p -> factoryClass.isAssignableFrom(p.getClass()))
                .collect(Collectors.toList());
    }

    /**
     * Filters for factories with matching context.
     *
     * @return all matching factories
     */
    private static <T extends PartitionerFactory> List<T> filterByFactoryIdentifier(Class<T> factoryClass,
                                                                                    String factoryIdentifier,
                                                                                    List<T> classFactories) {
        List<T> matchingFactories = classFactories.stream()
                .filter(f -> f.factoryIdentifier().equals(factoryIdentifier))
                .collect(Collectors.toList());

        if (matchingFactories.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Could not find any factory for identifier '%s' that implements '%s' in the classpath.\n\n" +
                                    "Available factory identifiers are:\n\n" +
                                    "%s",
                            factoryIdentifier,
                            factoryClass.getName(),
                            classFactories.stream()
                                    .map(PartitionerFactory::factoryIdentifier)
                                    .distinct()
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }
        if (matchingFactories.size() > 1) {
            throw new ValidationException(
                    String.format(
                            "Multiple factories for identifier '%s' that implement '%s' found in the classpath.\n\n" +
                                    "Ambiguous factory classes are:\n\n" +
                                    "%s",
                            factoryIdentifier,
                            factoryClass.getName(),
                            matchingFactories.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return matchingFactories;
    }
}
