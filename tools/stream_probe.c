#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static double now_seconds(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1000000000.0;
}

static double best_positive(double current, double candidate) {
    if (candidate <= 0.0) {
        return current;
    }
    if (current <= 0.0 || candidate < current) {
        return candidate;
    }
    return current;
}

int main(int argc, char **argv) {
    size_t total_mib = 512;
    const int iterations = 10;

    if (argc > 1) {
        char *end = NULL;
        unsigned long parsed = strtoul(argv[1], &end, 10);
        if (end != argv[1] && parsed > 0) {
            total_mib = (size_t)parsed;
        }
    }

    size_t total_bytes = total_mib * 1024ULL * 1024ULL;
    size_t n = total_bytes / (3ULL * sizeof(double));
    if (n < 1024) {
        fprintf(stderr, "array size is too small\n");
        return 2;
    }

    double *a = NULL;
    double *b = NULL;
    double *c = NULL;
    if (posix_memalign((void **)&a, 64, n * sizeof(double)) != 0 ||
        posix_memalign((void **)&b, 64, n * sizeof(double)) != 0 ||
        posix_memalign((void **)&c, 64, n * sizeof(double)) != 0) {
        fprintf(stderr, "allocation failed: %s\n", strerror(errno));
        free(a);
        free(b);
        free(c);
        return 3;
    }

    for (size_t i = 0; i < n; i++) {
        a[i] = 1.0;
        b[i] = 2.0;
        c[i] = 0.0;
    }

    double read_best = 0.0;
    double write_best = 0.0;
    double copy_best = 0.0;
    double scale_best = 0.0;
    double add_best = 0.0;
    double triad_best = 0.0;
    const double scalar = 3.0;
    volatile double guard = 0.0;

    for (int iter = 0; iter < iterations; iter++) {
        double start = now_seconds();
        double local_sum = 0.0;
        for (size_t i = 0; i < n; i++) {
            local_sum += a[i];
        }
        read_best = best_positive(read_best, now_seconds() - start);
        guard += local_sum;

        start = now_seconds();
        const double write_value = scalar + (double)iter;
        for (size_t i = 0; i < n; i++) {
            c[i] = write_value + (double)(i & 7) * 0.001;
        }
        write_best = best_positive(write_best, now_seconds() - start);
        guard += c[n / 2];

        start = now_seconds();
        for (size_t i = 0; i < n; i++) {
            c[i] = a[i];
        }
        copy_best = best_positive(copy_best, now_seconds() - start);

        start = now_seconds();
        for (size_t i = 0; i < n; i++) {
            b[i] = scalar * c[i];
        }
        scale_best = best_positive(scale_best, now_seconds() - start);

        start = now_seconds();
        for (size_t i = 0; i < n; i++) {
            c[i] = a[i] + b[i];
        }
        add_best = best_positive(add_best, now_seconds() - start);

        start = now_seconds();
        for (size_t i = 0; i < n; i++) {
            a[i] = b[i] + scalar * c[i];
        }
        triad_best = best_positive(triad_best, now_seconds() - start);
    }

    guard += a[n / 2] + b[n / 3] + c[n / 4];
    (void)guard;

    double mib = (double)n * sizeof(double) / (1024.0 * 1024.0);
    double one_vector_gb = ((double)n * sizeof(double)) / 1000000000.0;
    double copy_gb = (2.0 * (double)n * sizeof(double)) / 1000000000.0;
    double scale_gb = copy_gb;
    double add_gb = (3.0 * (double)n * sizeof(double)) / 1000000000.0;
    double triad_gb = add_gb;

    printf("{\n");
    printf("  \"status\": \"completed\",\n");
    printf("  \"array_mib_requested\": %zu,\n", total_mib);
    printf("  \"array_mib_per_vector\": %.3f,\n", mib);
    printf("  \"iterations\": %d,\n", iterations);
    printf("  \"metrics\": {\n");
    printf("    \"read_gb_s\": %.6f,\n", read_best > 0.0 ? one_vector_gb / read_best : 0.0);
    printf("    \"write_gb_s\": %.6f,\n", write_best > 0.0 ? one_vector_gb / write_best : 0.0);
    printf("    \"copy_gb_s\": %.6f,\n", copy_best > 0.0 ? copy_gb / copy_best : 0.0);
    printf("    \"scale_gb_s\": %.6f,\n", scale_best > 0.0 ? scale_gb / scale_best : 0.0);
    printf("    \"add_gb_s\": %.6f,\n", add_best > 0.0 ? add_gb / add_best : 0.0);
    printf("    \"triad_gb_s\": %.6f\n", triad_best > 0.0 ? triad_gb / triad_best : 0.0);
    printf("  }\n");
    printf("}\n");

    free(a);
    free(b);
    free(c);
    return 0;
}
