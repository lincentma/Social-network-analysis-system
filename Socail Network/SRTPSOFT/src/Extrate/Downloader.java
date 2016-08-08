package Extrate;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Downloader {	
	private final static String userAgent = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)";
    private static String defaultEncoding = "utf-8";
    private final static Pattern pContentType = Pattern.compile("content=[\"\']?[^\'\">]+?charset=[\"\']?([^\'\">]+)", Pattern.CASE_INSENSITIVE);
    private final static Pattern pXmlEncoding = Pattern.compile("(?i)encoding=[\"']([^\"'\\?\\s]+)");
    private final static int downloadDataLimit = 500000;
    private final static int contimeout = 500;
    private final static int readtimeout = 5000;
    private final static int loopCount = 3;
    private static boolean loopcontinue = true;
    static String charsetvalue = "UTF-8|EFBBBF@UTF-16|FEFF@UTF-16|FFFE@UTF-32|0000FEFF@UTF-32|FFFE0000@UTF-1|F7644C@UTF-EBCDIC|DD736673@SCSU|0EFEFF@BOCU-1|FBEE28@GB-18030|84319533";
    public static Map<String, String> Encodemap = null;
    private String requestURL = "";

    public String getRequestURL() {
        return requestURL;
    }

    public void setRequestURL(String requestURL) {
        this.requestURL = requestURL;
    }

    private static synchronized void initMap() {
        if (Encodemap == null) {
            Encodemap = new HashMap<String, String>();
            for (String sv : charsetvalue.split("@"))
                Encodemap.put(sv.substring(sv.indexOf("|") + 1).toLowerCase(), sv.substring(0, sv.indexOf("|")).toLowerCase());
        }
    }

    public Downloader() {
        initMap();
    }

    public Downloader(String defaultEncoding) {
        this.defaultEncoding = defaultEncoding;
        initMap();
    }    
    
    public String getHTMLString(String url, boolean usedefaultEncoding) {
        return getHTMLString(url, usedefaultEncoding, true);
    }

    public String getHTMLString(String url, boolean usedefaultEncoding, boolean isRedirect) {
        StringBuilder htmlContent = new StringBuilder();
        try {
            String encoding = null;
            byte[] buff = null;
            for (int i = 0; i <= loopCount; i++) {
                if (!loopcontinue)
                    break;
                HttpURLConnection huc = getURLConnection(url, isRedirect);
                setRequestURL(huc == null ? "" : huc.getURL().toString());
                if (huc == null)
                    continue;
                buff = getWebContent(huc, url);
                if (buff == null)
                {
                	continue;
                }
                encoding = getEncoding(huc, buff, usedefaultEncoding, url);
                break;
            }            
            if (buff != null)
            {
            	htmlContent.append(decode(buff, encoding));
            }

        } 
        catch (Exception e) {
        	return null;
        }
        return htmlContent.toString();
    }

    public String getFileEncode(byte[] buff, String url) {
        StringBuffer sb = new StringBuffer("");
        int i = 0;
        for (byte b : buff) {
            if (i++ > 10)
                break;
            sb.append(Integer.toHexString(b & 0XFF).toLowerCase());
            if (Encodemap.get(sb.toString()) != null)
                return Encodemap.get(sb.toString());
        }
        return null;
    }

    public String getEncoding(HttpURLConnection huc, byte[] buff, boolean usedefaultEncoding, String url) {
        if (usedefaultEncoding)
            return defaultEncoding;
        String fileencode = getFileEncode(buff, url);
        if (fileencode != null) {
            return fileencode;
        }
        String encoding = null;
        String contenttype = huc.getHeaderField("Content-Type") == null ? "" : huc.getHeaderField("Content-Type").toLowerCase();
        if (!contenttype.equals("")) {
            if (contenttype.indexOf("charset=") != -1) {
                encoding = contenttype.substring(contenttype.lastIndexOf("=") + 1);
                encoding = getVerifyEncoding(encoding, false);
            }
        }
        if (encoding != null)
            return encoding;
        return getEncodingFromContent(buff,url);
    }

    public String getVerifyEncoding(String encoding, boolean setDefault) {
        try {
            encoding = Charset.forName(encoding).name();
        } catch (IllegalArgumentException iae) {
            if (setDefault)
                encoding = defaultEncoding;
            else
                encoding = null;
        }
        return encoding;
    }

    public String getEncodingFromContent(byte[] buff,String url) {
        String encoding = defaultEncoding;
        int len = buff.length;
        byte[] dest = new byte[len < 1024 ? len : 1024];
        System.arraycopy(buff, 0, dest, 0, dest.length);
        String header = new String(dest);
        if(url.endsWith(".xml")){
        	Matcher m = pXmlEncoding.matcher(header);
        	if(m.find()){
        		encoding = m.group(1);
        		return getVerifyEncoding(encoding, true);
        	}
        }
        String[] metas = header.split("(i)<meta ");
        for (String meta : metas) {
            String metslow = meta.toLowerCase();
            if (metslow.indexOf("http-equiv") != -1 && metslow.indexOf("content") != -1) {
                Matcher m = pContentType.matcher(header);
                if (m.find()) {
                    encoding = m.group(1);
                    return getVerifyEncoding(encoding, true);
                }
            }
        }
        return getVerifyEncoding(encoding, true);
    }

    public byte[] getWebContent(HttpURLConnection huc, String url) {
        int CHUNK_LENGTH = 4096;
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream(CHUNK_LENGTH * 2);
            byte[] buff = new byte[CHUNK_LENGTH];
            InputStream in = huc.getInputStream();
            int count ;
            int downStopCount = 0;
            do {
            	count = in.read(buff);
            	if(count<=0)
            		break;
                buffer.write(buff, 0, count);
                downStopCount += count;
                if ( downStopCount > downloadDataLimit)
                    break;
                
            } while (true);
            buffer.close();
            in.close();
            huc.disconnect();
            if (downStopCount > downloadDataLimit) {
                return null;
            }
            buff = buffer.toByteArray();
            return buff;
        }
        catch(SocketTimeoutException e){
        	System.out.println(url);
        	return null;
        }
        catch (Exception e) {
        	e.printStackTrace();
            String message = e.getMessage();
            if ("Read timed out".equals(message)) {
            	System.out.println(url);
            } else {
                loopcontinue = false;
            }
        }
        return null;
    }

    public HttpURLConnection getURLConnection(String url, boolean isRedirect) {
        HttpURLConnection huc = null;
        int httpCode = -1;
        try {
            URL u = new URL(url);
            URLConnection uc = u.openConnection();
            uc.setRequestProperty("User-Agent", userAgent);
            uc.setRequestProperty("Accept-Language", "zh-cn,zh;q=0.5");
            uc.setConnectTimeout(contimeout);
            uc.setReadTimeout(readtimeout);
            huc = (HttpURLConnection) uc;
            HttpURLConnection.setFollowRedirects(false);
            huc.setInstanceFollowRedirects(isRedirect);
            try {
                httpCode = huc.getResponseCode();
            } catch (Exception e) {
                if (huc != null)
                    huc.disconnect();
                String message = e.getMessage();
                if ("Read timed out".equals(message) || "connect timed out".equals(message)) {
                	System.out.println(url + " - " + message);
                } else {
                    loopcontinue = false;
                    throw e;
                }
            }
            if (httpCode >= 200 && httpCode < 400 && httpCode != 301) {
                return huc;
            } else {
                huc.disconnect();
                return null;
            }
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            System.out.println(url + " " + sw.toString());
        }
        return null;
    }

    private CharSequence decode(byte[] buffer, String encoding) {
    	if("gb2312".equalsIgnoreCase(encoding)){
    		encoding = "gbk";
    	}
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        int size = buffer.length;
        bb.limit((int) size);
        CharBuffer content = (Charset.forName(encoding)).decode(bb);
        return content;
    }
    
    public static void main(String args[]) {
        String url = "http://www.chinapop.gov.cn/";        
        Downloader d = new Downloader();
        String htmlStr = d.getHTMLString(url, false);
        System.out.println(htmlStr);
    }
}
