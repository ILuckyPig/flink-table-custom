package com.lu.table.factories;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class ConcurrencyReadFactoryService {
    private static final Logger LOG = LoggerFactory.getLogger(ConcurrencyReadFactoryService.class);

    /**
     * Finds a table factory of the given class and property map.
     *
     * @param factoryClass desired factory class
     * @param propertyMap properties that describe the factory configuration
     * @param <T> factory class type
     * @return the matching factory
     */
    public static <T extends ConcurrencyReadFactory> T find(Class<T> factoryClass, Map<String, String> propertyMap) {
        return findSingleInternal(factoryClass, propertyMap, Optional.empty());
    }

    /**
     * Finds a table factory of the given class, property map, and classloader.
     *
     * @param factoryClass desired factory class
     * @param properties properties that describe the factory configuration
     * @param classLoader classloader for service loading
     * @param <T> factory class type
     * @return the matching factory
     */
    private static <T extends ConcurrencyReadFactory> T findSingleInternal(
            Class<T> factoryClass,
            Map<String, String> properties,
            Optional<ClassLoader> classLoader) {

        List<ConcurrencyReadFactory> readPartitionFactories = discoverFactories(classLoader);
        List<T> filtered = filter(readPartitionFactories, factoryClass, properties);

        if (filtered.size() > 1) {
            throw new RuntimeException(
                    filtered +
                    factoryClass.toString() +
                    readPartitionFactories +
                    properties);
        } else {
            return filtered.get(0);
        }
    }

    /**
     * Searches for factories using Java service providers.
     *
     * @return all factories in the classpath
     */
    private static List<ConcurrencyReadFactory> discoverFactories(Optional<ClassLoader> classLoader) {
        try {
            List<ConcurrencyReadFactory> result = new LinkedList<>();
            ClassLoader cl = classLoader.orElse(Thread.currentThread().getContextClassLoader());
            ServiceLoader
                    .load(ConcurrencyReadFactory.class, cl)
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
    private static <T extends ConcurrencyReadFactory> List<T> filter(
            List<ConcurrencyReadFactory> foundFactories,
            Class<T> factoryClass,
            Map<String, String> properties) {

        Preconditions.checkNotNull(factoryClass);
        Preconditions.checkNotNull(properties);

        List<T> classFactories = filterByFactoryClass(
                factoryClass,
                properties,
                foundFactories);

        List<T> contextFactories = filterByContext(
                factoryClass,
                properties,
                classFactories);

        return filterBySupportedProperties(
                factoryClass,
                properties,
                classFactories,
                contextFactories);
    }

    /**
     * Filters factories with matching context by factory class.
     */
    @SuppressWarnings("unchecked")
    private static <T> List<T> filterByFactoryClass(
            Class<T> factoryClass,
            Map<String, String> properties,
            List<ConcurrencyReadFactory> foundFactories) {

        List<ConcurrencyReadFactory> classFactories = foundFactories.stream()
                .filter(p -> factoryClass.isAssignableFrom(p.getClass()))
                .collect(Collectors.toList());

        if (classFactories.isEmpty()) {
            throw new RuntimeException(
                    String.format("No factory implements '%s'.", factoryClass.getCanonicalName()) +
                    factoryClass +
                    foundFactories +
                    properties);
        }

        return (List<T>) classFactories;
    }

    /**
     * Filters for factories with matching context.
     *
     * @return all matching factories
     */
    private static <T extends ConcurrencyReadFactory> List<T> filterByContext(
            Class<T> factoryClass,
            Map<String, String> properties,
            List<T> classFactories) {
        List<T> matchingFactories = new ArrayList<>();
        for (T factory : classFactories) {
            Map<String, String> requestedContext = normalizeContext(factory);

            Map<String, String> plainContext = new HashMap<>(requestedContext);

            // check if required context is met
            Map<String, Tuple2<String, String>> missMatchedProperties = new HashMap<>();
            Map<String, String> missingProperties = new HashMap<>();
            for (Map.Entry<String, String> e : plainContext.entrySet()) {
                if (properties.containsKey(e.getKey())) {
                    String fromProperties = properties.get(e.getKey());
                    if (!Objects.equals(fromProperties, e.getValue())) {
                        missMatchedProperties.put(e.getKey(), new Tuple2<>(e.getValue(), fromProperties));
                    }
                } else {
                    missingProperties.put(e.getKey(), e.getValue());
                }
            }
            int matchedSize = plainContext.size() - missMatchedProperties.size() - missingProperties.size();
            if (matchedSize == plainContext.size()) {
                matchingFactories.add(factory);
            }
        }

        if (matchingFactories.isEmpty()) {
            throw new RuntimeException(String.format("Required context properties mismatch. %s, %s, %s",
                    factoryClass, classFactories, properties));
        }

        return matchingFactories;
    }



    /**
     * Prepares the properties of a context to be used for match operations.
     */
    private static Map<String, String> normalizeContext(ConcurrencyReadFactory factory) {
        Map<String, String> requiredContext = factory.requiredContext();
        if (requiredContext == null) {
            throw new RuntimeException(
                    String.format("Required context of factory '%s' must not be null.", factory.getClass().getName()));
        }
        return requiredContext.keySet().stream().collect(Collectors.toMap(String::toLowerCase, requiredContext::get));
    }

    /**
     * Filters the matching class factories by supported properties.
     */
    private static <T extends ConcurrencyReadFactory> List<T> filterBySupportedProperties(
            Class<T> factoryClass,
            Map<String, String> properties,
            List<T> classFactories,
            List<T> contextFactories) {
        List<T> supportedFactories = new LinkedList<>();
        for (T factory : contextFactories) {
            List<String> supportedProperties = normalizeSupportedProperties(factory);

            List<String> plainProperties = new ArrayList<>(supportedProperties);

            for (String plainProperty : plainProperties) {
                if (!properties.containsKey(plainProperty)) {
                    throw new RuntimeException(String.format("supported property key {%s} mismatch. %s, %s, %s",
                            plainProperty, factoryClass, classFactories, properties));
                }
            }
            supportedFactories.add(factory);
        }
        return supportedFactories;
    }

    /**
     * Prepares the supported properties of a factory to be used for match operations.
     */
    private static List<String> normalizeSupportedProperties(ConcurrencyReadFactory factory) {
        List<String> supportedProperties = factory.supportedProperties();
        if (supportedProperties == null) {
            throw new RuntimeException(
                    String.format("Supported properties of factory '%s' must not be null.",
                            factory.getClass().getName()));
        }

        return supportedProperties.stream().map(String::toLowerCase).collect(Collectors.toList());
    }
}
