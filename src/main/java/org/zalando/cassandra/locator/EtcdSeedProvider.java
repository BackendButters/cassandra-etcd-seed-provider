package org.zalando.cassandra.locator;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import org.apache.cassandra.locator.SeedProvider;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.cassandra.locator.SeedDescriptor;
import org.zalando.cassandra.locator.etcd.KeyNode;
import org.zalando.cassandra.locator.etcd.ResultNode;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class EtcdSeedProvider implements SeedProvider {
    private static final Logger logger = LoggerFactory.getLogger(EtcdSeedProvider.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private final URL etcdUrl;

    public EtcdSeedProvider(final Map<String, String> args) {
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        final Optional<String> url = Optional.fromNullable(args.get("url"));
        try {
            if (url.isPresent()) {
                etcdUrl = new URL(url.get());
            } else {
                logger.warn("Seed provider doesn't have any parameters");
                etcdUrl = new URL(System.getenv("ETCD_URL"));
            }
        } catch (MalformedURLException e) {
            logger.error("Invalid etcd url", e);
            throw new AssertionError(e);
        }
    }



    private LoadingCache<URL, List<InetAddress>> seeds = CacheBuilder.newBuilder()
            .expireAfterWrite(5L, TimeUnit.SECONDS)
            .build(
                    new CacheLoader<URL, List<InetAddress>>() {
                        public List<InetAddress> load(final URL url) {
                            try {
                                final ResultNode result = mapper.readValue(etcdUrl, ResultNode.class);
                                logger.debug("Refreshing seed list ...");
                                return buildSeedList(result);
                            } catch (java.io.IOException e) {
                                logger.error("Failed to get seed list from etcd", e);
                            }
                            return ImmutableList.of();
                        }
                    });



    public EtcdSeedProvider(final URL etcdUrl) {
        this.etcdUrl = etcdUrl;
    }

    public List<InetAddress> getSeeds() {
        return seeds.getUnchecked(etcdUrl);
    }

    private List<InetAddress> buildSeedList(final ResultNode result) {
        if (result == null) {
            return ImmutableList.of();
        }

        return FluentIterable.from(result.getNode().getNodes())
                .transform(KEYNODE_TO_INETADDRESS)
                .filter(Predicates.notNull())
                .toList();
    }

    private final String hostnameFromDescriptor(final String value) {
        if (value.startsWith("{")) {
            try {
                return mapper.readValue(value, SeedDescriptor.class).getHost();
            } catch (java.io.IOException e) {
                logger.error("Failed to parse seed descriptor", e);
                return null;
            }
        } else {
            return value;
        }
    }

    private final Function<KeyNode, InetAddress> KEYNODE_TO_INETADDRESS = new Function<KeyNode, InetAddress>() {
        public InetAddress apply(final KeyNode keyNode) {
            final String hostname = hostnameFromDescriptor(keyNode.getValue());
            if (hostname != null) {
                try {
                    return InetAddress.getByName(hostname);
                } catch (UnknownHostException e) {
                    logger.error("Invalid hostname {}", hostname);
                }
            }
            return null;
        }
    };

}
