/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.streaming.connectors.cassandra.example;

import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Embedded cassandra daemon running in background as service.
 */
public class EmbeddedCassandraService {

	private static final Logger LOG = LoggerFactory.getLogger(EmbeddedCassandraService.class);

	private File tmpDir;

	private static final String CAS_CONFIG_PROP = "cassandra.config";

	/**
	 * Holding the ref of embedded cassandra daemon.
	 */
	private AtomicReference<CassandraDaemon> cassandraDaemonRef = new AtomicReference<>();

	private ExecutorService cassandraDaemonExecutor = Executors.newCachedThreadPool();

	private CountDownLatch startUpLatch = new CountDownLatch(1);

	private static final boolean SELF_MANAGED_LIFECYCLE = true;

	public EmbeddedCassandraService() {

	}

	public void start() {
		cassandraDaemonExecutor.execute(new Runnable() {
			@Override
			public void run() {
				try {
				    //Set this false or default will make C* daemon to System.exit(0) in incomplete state.
					CassandraDaemon cassandraDaemon = new CassandraDaemon(SELF_MANAGED_LIFECYCLE);
					initConfig();
					//NOTE: Use the following 2 method for C* 2.x would hang the main thread?
//					cassandraDaemon.completeSetup();
//					cassandraDaemon.activate();
					cassandraDaemon.init(null);
					cassandraDaemon.start();

					cassandraDaemonRef.getAndSet(cassandraDaemon);
					startUpLatch.countDown();
					LOG.info("Embedded Cassasndra service started");
				} catch (IOException e) {
					LOG.error("Failed to start Embedded Cassandra Service, due to: ", e);
				}
			}
		});

		try {
			startUpLatch.await(30, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			LOG.error("Timeout starting Embedded Cassandra Service");
		}

		addShutdownHook();
	}

	private void addShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				CassandraDaemon cassandraDaemon = cassandraDaemonRef.get();

				LOG.info("Calling stop() on Embedded Cassandra Daemon");
				cassandraDaemon.deactivate();

				LOG.info("Shutting down Embedded Cassandra Service");
				cassandraDaemonExecutor.shutdownNow();

				LOG.info("Embedded Cassasndra has fully stopped");

				if (tmpDir != null) {
					//noinspection ResultOfMethodCallIgnored
					tmpDir.delete();
					LOG.info("Temp Cassandra storage deleted");
				}
			}
		});
	}

	private void initConfig() throws IOException {
		// generate temporary files
		tmpDir = createTempDirectory();
		ClassLoader classLoader = EmbeddedCassandraService.class.getClassLoader();
		InputStream in = getClass().getResourceAsStream("/cassandra.yaml");
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		File tmp = new File(tmpDir.getAbsolutePath() + File.separator + "cassandra.yaml");

//		Assert.assertTrue(tmp.createNewFile());

		BufferedWriter b = new BufferedWriter(new FileWriter(tmp));
		//copy cassandra.yaml; inject absolute paths into cassandra.yaml
		String line;
		while ((line = reader.readLine()) != null) {
			line = line.replace("$PATH", "'" + tmp.getParentFile());
			b.write(line + "\n");
			b.flush();
		}

		// Tell cassandra where the configuration files are.
		// Use the test configuration file.
		System.setProperty(CAS_CONFIG_PROP, tmp.getAbsoluteFile().toURI().toString());

		LOG.info("Embedded Cassasndra config generated at [{}]", System.getProperty(CAS_CONFIG_PROP));
	}

	private static File createTempDirectory() throws IOException {
		File tempDir = new File(System.getProperty("java.io.tmpdir"));

		for (int i = 0; i < 10; i++) {
			File dir = new File(tempDir, UUID.randomUUID().toString());
			if (!dir.exists() && dir.mkdirs()) {
				LOG.debug("Creating temp dir [{}]", dir.getAbsolutePath());
				return dir;
			}
		}

		throw new IOException("Could not create temporary file directory");
	}

}
