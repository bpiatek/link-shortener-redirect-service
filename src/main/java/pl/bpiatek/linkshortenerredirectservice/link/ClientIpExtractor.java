package pl.bpiatek.linkshortenerredirectservice.link;

import jakarta.servlet.http.HttpServletRequest;

public final class ClientIpExtractor {

    private ClientIpExtractor() {
    }

    public static String extract(HttpServletRequest request) {
        if (request == null) {
            return "UNKNOWN";
        }

        var xfHeader = request.getHeader("X-Forwarded-For");
        if (xfHeader != null && !xfHeader.isEmpty()) {
            return xfHeader.split(",")[0].trim();
        }

        var cfHeader = request.getHeader("CF-Connecting-IP");
        if (cfHeader != null && !cfHeader.isEmpty()) {
            return cfHeader;
        }

        return request.getRemoteAddr();
    }
}
