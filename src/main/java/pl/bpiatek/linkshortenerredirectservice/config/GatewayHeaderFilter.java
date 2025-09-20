package pl.bpiatek.linkshortenerredirectservice.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

@Component
class GatewayHeaderFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        if (request.getRequestURI().startsWith("/actuator")) {
            filterChain.doFilter(request, response);
            return;
        }

        var userIdHeader = request.getHeader("X-User-Id");

        if (userIdHeader == null || userIdHeader.isBlank()) {
            response.setStatus(HttpStatus.FORBIDDEN.value());
            response.getWriter().write("Request not authorized. Missing required header.");
            return;
        }

        filterChain.doFilter(request, response);
    }
}