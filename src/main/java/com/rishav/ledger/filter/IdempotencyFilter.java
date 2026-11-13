package com.rishav.ledger.filter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingResponseWrapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class IdempotencyFilter extends OncePerRequestFilter {

    private static final String IDEMPOTENCY_HEADER = "Idempotency-Key";
    private static final String TRANSFER_PATH = "/api/v1/transfers";
    private static final Duration TTL = Duration.ofHours(24);

    private final StringRedisTemplate redisTemplate;

    public IdempotencyFilter(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {

        // Only intercept POST requests to /api/v1/transfers
        if (!"POST".equalsIgnoreCase(request.getMethod()) ||
                !request.getRequestURI().equals(TRANSFER_PATH)) {
            filterChain.doFilter(request, response);
            return;
        }

        String idempotencyKey = request.getHeader(IDEMPOTENCY_HEADER);
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.setContentType("application/json");
            response.getWriter().write("{\"error\":\"Missing Idempotency-Key header\"}");
            return;
        }

        String lockKey = "idempotency:" + idempotencyKey;
        String responseKey = "idempotency:response:" + idempotencyKey;

        // Try to set the idempotency key (SETNX semantics)
        Boolean isNew = redisTemplate.opsForValue().setIfAbsent(lockKey, "1", TTL);

        if (Boolean.FALSE.equals(isNew)) {
            // Key already exists — return cached response
            String cachedResponse = redisTemplate.opsForValue().get(responseKey);
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json");
            if (cachedResponse != null) {
                response.getWriter().write(cachedResponse);
            }
            return;
        }

        // Key is new — proceed with the request and cache the response
        ContentCachingResponseWrapper responseWrapper = new ContentCachingResponseWrapper(response);
        try {
            filterChain.doFilter(request, responseWrapper);

            // Cache the response body
            byte[] body = responseWrapper.getContentAsByteArray();
            if (body.length > 0) {
                String responseBody = new String(body, StandardCharsets.UTF_8);
                redisTemplate.opsForValue().set(responseKey, responseBody, TTL);
            }

            responseWrapper.copyBodyToResponse();
        } catch (Exception ex) {
            // Clean up the idempotency key on failure so it can be retried
            redisTemplate.delete(lockKey);
            throw ex;
        }
    }
}
