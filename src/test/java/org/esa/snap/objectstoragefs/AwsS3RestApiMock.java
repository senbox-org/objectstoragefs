package org.esa.snap.objectstoragefs;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;

public class AwsS3RestApiMock {

    private Server server;
    private Map<String, File> files = new HashMap<>();
    private long nextRequestId = new Random().nextLong();

    public static void main(String[] args) throws Exception {
        AwsS3RestApiMock mock = new AwsS3RestApiMock();
        mock.start(8080);
        mock.server.join();
    }

    static int[] parseRange(String rangeSpec, int[] defaultRange) {
        final String rangePrefix = "bytes=";
        int[] value = Arrays.copyOf(defaultRange, 2);
        if (!rangeSpec.startsWith(rangePrefix)) {
            throw new IllegalArgumentException("rangeSpec");
        }
        String s = rangeSpec.substring(rangePrefix.length());
        String[] parts = s.split("-", 2);
        for (int i = 0; i < value.length; i++) {
            String part = parts[i].trim();
            if (!part.isEmpty()) {
                value[i] = Integer.parseInt(part);
            }
        }
        if (value[0] > value[1]) {
            throw new IllegalArgumentException("rangeSpec");
        }
        return value;
    }

    @SuppressWarnings("WeakerAccess")
    void addFile(String key, String lastModified, String contentType, byte[] data) {
        File file = new File(key, lastModified, contentType, data);
        files.put(file.key, file);
    }

    void start(int port) throws Exception {
        loadFiles();
        server = new Server(port);
        server.setHandler(new AwsS3RestApiHandler());
        server.start();
        //server.join();
    }

    void stop() throws Exception {
        server.stop();
    }

    private void loadFiles() {
        addFile("index.html", "2016-08-26T20:26:33.000Z", "text/html;charset=utf-8", "<html/>".getBytes());
        addFile("style.css", "2015-12-16T12:46:19.000Z", "text/css;charset=utf-8", "".getBytes());
        for (int i = 0; i < 3; i++) {
            String prefix = "products/201" + (5 + i) + "/10/1/S2A_OPER_PRD_MSIL1C_PDMC_20160729T004231_R007_V20151001T091034_20151001T091034/";
            String lastModified = "2016-07-13T17:24:" + (10 + i) + ".000Z";
            addFile(prefix + "preview/B01.jp2", lastModified, "application/binary", new byte[116665]);
            addFile(prefix + "preview/B02.jp2", lastModified, "application/binary", new byte[116665]);
            addFile(prefix + "preview/B03.jp2", lastModified, "application/binary", new byte[116665]);
            addFile(prefix + "metadata.xml", lastModified, "text/xml;charset=utf-8", "<Metadata/>".getBytes());
        }
        for (int j = 1; j <= 3; j++) {
            for (int i = 0; i < 10; i++) {
                String prefix = "tiles/" + j + "/C/CV/2015/12/25/" + i + "/";
                String lastModified = "2016-07-13T17:24:" + (10 + i * j) + ".000Z";
                addFile(prefix + "B01.jp2", lastModified, "application/binary", new byte[1024 * 1024]);
                addFile(prefix + "B02.jp2", lastModified, "application/binary", new byte[1024 * 1024]);
                addFile(prefix + "B03.jp2", lastModified, "application/binary", new byte[1024 * 1024]);
                addFile(prefix + "metadata.xml", lastModified, "text/xml;charset=utf-8", "<Metadata/>".getBytes());
                addFile(prefix + "preview.jpg", lastModified, "image/jpeg", new byte[116665]);
            }
        }
    }

    private String toContents(File file) {
        return String.format("" +
                                     "<Contents>\n" +
                                     "  <Key>%s</Key>\n" +
                                     "  <LastModified>%s</LastModified>\n" +
                                     "  <Size>%d</Size>\n" +
                                     "  <ETag>\"5093fa512c4aa58b5f080da62f4b00dc\"</ETag>\n" +
                                     "  <Owner>\n" +
                                     "    <ID>91d380b3cead28df927c824731b0173701336cd8d67b0679d7166288f3850f38</ID>\n" +
                                     "  </Owner>\n" +
                                     "  <StorageClass>STANDARD</StorageClass>\n" +
                                     "</Contents>\n",
                             file.key, file.lastModified, file.data.length);
    }

    private String toCommonPrefix(String prefix) {
        return String.format("" +
                                     "<CommonPrefixes>\n" +
                                     "  <Prefix>%s</Prefix>\n" +
                                     "</CommonPrefixes>\n", prefix);
    }

    public static class File {
        final String key;
        final String lastModified;
        final String contentType;
        final byte[] data;

        File(String key, String lastModified, String contentType, byte[] data) {
            this.key = key;
            this.lastModified = lastModified;
            this.contentType = contentType;
            this.data = data;
        }
    }

    class AwsS3RestApiHandler extends AbstractHandler {
        @Override
        public void handle(String key,
                           Request request,
                           HttpServletRequest httpServletRequest,
                           HttpServletResponse httpServletResponse) throws IOException, ServletException {

            nextRequestId++;

            String prefix = httpServletRequest.getParameter("prefix");
            String delimiter = httpServletRequest.getParameter("delimiter");

            //System.out.println("AwsS3RestApiHandler:");
            //System.out.println("  key = " + key);
            //System.out.println("  prefix = " + prefix);
            //System.out.println("  delimiter = " + delimiter);

            if (key.equals("/")) {
                StringBuffer result = new StringBuffer("" +
                                                               "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n" +
                                                               "  <Name>TEST</Name>\n" +
                                                               "  <Marker/>\n" +
                                                               "  <MaxKeys>1000</MaxKeys>\n" +
                                                               "  <IsTruncated>false</IsTruncated>");
                if (prefix != null) {
                    result.append(String.format("  <Prefix>%s</Prefix>\n", prefix));
                }
                if (delimiter != null) {
                    result.append(String.format("  <Delimiter>%s</Delimiter>\n", delimiter));
                }

                if (prefix == null || prefix.trim().isEmpty()) {
                    prefix = "";
                }
                if (delimiter == null || delimiter.trim().isEmpty()) {
                    delimiter = "/";
                }

                ArrayList<String> keyList = new ArrayList<>();
                HashSet<String> commonPrefixes = new HashSet<>();
                if (delimiter.isEmpty() && prefix.isEmpty()) {
                    keyList.addAll(files.keySet());
                } else {
                    for (File file : files.values()) {
                        if (file.key.startsWith(prefix)) {
                            int index = file.key.indexOf(delimiter, prefix.length());
                            if (index < 0) {
                                keyList.add(file.key);
                            } else {
                                commonPrefixes.add(file.key.substring(0, index + 1));
                            }
                        }
                    }
                }
                Collections.sort(keyList);
                for (String fileKey : keyList) {
                    result.append(toContents(files.get(fileKey)));
                }
                ArrayList<String> cpList = new ArrayList<>(commonPrefixes);
                Collections.sort(cpList);
                for (String commonPrefix : cpList) {
                    result.append(toCommonPrefix(commonPrefix));
                }
                result.append("</ListBucketResult>");
                httpServletResponse.setStatus(HttpServletResponse.SC_OK);
                httpServletResponse.setContentType("text/xml;charset=utf-8");
                httpServletResponse.getWriter().println(result);
            } else {
                File file = files.get(key.substring(1));
                if (file != null) {
                    String rangeSpec = httpServletRequest.getHeader("Range");
                    int[] range = new int[]{0, file.data.length - 1};
                    if (rangeSpec != null) {
                        //System.out.println("  range = " + rangeSpec);
                        try {
                            range = parseRange(rangeSpec, range);
                        } catch (IllegalArgumentException e) {
                            httpServletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                            return;
                        }
                    }
                    int offset = range[0];
                    int length = 1 + range[1] - offset;
                    if (offset == 0 && length == file.data.length) {
                        httpServletResponse.setStatus(HttpServletResponse.SC_OK);
                    } else {
                        httpServletResponse.setStatus(HttpServletResponse.SC_PARTIAL_CONTENT);
                    }
                    httpServletResponse.setContentType(file.contentType);
                    httpServletResponse.setContentLength(file.data.length);
                    httpServletResponse.setHeader("Last-Modified", file.lastModified);
                    httpServletResponse.setHeader("Accept-Ranges", "bytes");
                    httpServletResponse.getOutputStream().write(file.data, offset, length);
                } else {
                    httpServletResponse.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    httpServletResponse.setContentType("text/xml;charset=utf-8");
                    httpServletResponse.getWriter().println(String.format("" +
                                                                                  "<Error>\n" +
                                                                                  "  <Code>NoSuchKey</Code>\n" +
                                                                                  "  <Message>The specified key does not exist.</Message>\n" +
                                                                                  "  <Key>%s</Key>\n" +
                                                                                  "  <RequestId>%s</RequestId>\n" +
                                                                                  "  <HostId>%s</HostId>\n" +
                                                                                  "</Error>",
                                                                          key,
                                                                          Long.toHexString(nextRequestId),
                                                                          Integer.toHexString(this.hashCode())));

                }
            }
            httpServletResponse.flushBuffer();
        }
    }
}
