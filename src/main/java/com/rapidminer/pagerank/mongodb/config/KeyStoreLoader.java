package com.rapidminer.pagerank.mongodb.config;


import java.math.*;
import java.io.*;
import java.security.cert.*;
import java.security.*;
import javax.net.ssl.*;
import java.util.*;

public class KeyStoreLoader
{
    private static Set<String> checksumCache;
    
    public static void addKeyStoreToTrustStore(final String keystorePath, final String keystorePassword) throws NoSuchAlgorithmException, IOException, KeyManagementException, KeyStoreException, CertificateException {
        if (keystorePath != null && !keystorePath.isEmpty()) {
            final MessageDigest digest = MessageDigest.getInstance("MD5");
            final File f = new File(keystorePath);
            try (final InputStream is = new FileInputStream(f)) {
                final byte[] buffer = new byte[8192];
                int read = 0;
                while ((read = is.read(buffer)) > 0) {
                    digest.update(buffer, 0, read);
                }
                final byte[] md5sum = digest.digest();
                final BigInteger bigInt = new BigInteger(1, md5sum);
                final String output = bigInt.toString(16);
                if (!KeyStoreLoader.checksumCache.contains(output)) {
                    addTrustStore(keystorePath, keystorePassword);
                    KeyStoreLoader.checksumCache.add(output);
                }
            }
        }
    }
    
    private static void addTrustStore(final String trustStore, final String password) throws NoSuchAlgorithmException, KeyStoreException, CertificateException, KeyManagementException, IOException {
        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("X509");
        final KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (final FileInputStream keystoreStream = new FileInputStream(trustStore)) {
            keystore.load(keystoreStream, password.toCharArray());
            trustManagerFactory.init(keystore);
            final TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
            final SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustManagers, null);
            SSLContext.setDefault(sc);
        }
    }
    
    static {
        KeyStoreLoader.checksumCache = new HashSet<String>(1, 1.0f);
    }
}
