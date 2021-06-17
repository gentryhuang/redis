#ifndef __GEO_H__
#define __GEO_H__

#include "server.h"

/* Structures used inside geo.c in order to represent points and array of
 * points on the earth.
 *
 * 位置信息的阵列的结构
 */
typedef struct geoPoint {
    // 经度
    double longitude;
    // 维度
    double latitude;
    double dist;
    double score;
    char *member;
} geoPoint;

typedef struct geoArray {
    struct geoPoint *array;
    size_t buckets;
    size_t used;
} geoArray;

#endif
