/*
 * Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
 * Copyright (c) 2014, Matt Stancliff <matt@genges.com>.
 * Copyright (c) 2015, Salvatore Sanfilippo <antirez@gmail.com>.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef GEOHASH_H_
#define GEOHASH_H_

#include <stddef.h>
#include <stdint.h>
#include <stdint.h>

/*
 * 1 Redis 采用了业界广泛使用的 GeoHash 编码方法，这个方法的基本原理就是 "二分区间，区间编码"。
 *   1.1 二分区间：对经度/维度范围不断进行二分，二分的次数可以定义
 *   1.2 区间编码：目标经度/维度落在二分后的两个区间的左边，则记为 0 ，落在右边则记为 1 。
 *   1.3 例子：经度范围是 [-180,180] ，GeoHash 编码会把目标经度值编码成一个 N 位的二进制值。目标经度值 116.37，我们采用 3 位编码值（也就是 N = 3，做 3 次二分区）。
 *       - 第一次二分区：把经度区间[-180,180]分成了左分区[-180,0) 和右分区[0,180]，此时，经度值 116.37 是属于右分区[0,180]，所以，我们用 1 表示第一次二分区后的编码值。
 *       - 第二次二分区：把经度值 116.37 所属的[0,180]区间，分成[0,90) 和[90, 180]。此时，经度值 116.37 还是属于右分区[90,180]，所以，第二次分区后的编码值仍然为 1。
 *       - 第三次二分区：把经度值 116.37 所属的[90,180]区间，分成[90,135) 和 [135,180]。此时，经度值 116.37 还是属于左分区[90,135)，所以，第三次分区后的编码值仍然为 0。
 *       按照这种方法，做完 3 次分区后，我们把经度值 116.37 定位在[90,135)这个区间，并且得到了经度值的 3 位编码值，即 110。
 *       对纬度的编码方式，和对经度的一样，只是纬度的范围是[-85.05112878，85.05112878]。
 *
 * 2 当对目标经纬度进行 GeoHash 编码时，先对经度和维度分别编码，然后再把经纬度各自的编码组成一个最终编码，组合的规则是：最终编码值的偶数位上依次是经度的编码值，奇数位上依次是纬度的编码值，其中，偶数位从 0 开始，奇数位从 1 开始。
 *   2.1 例子：经纬度（116.37，39.86）的各自编码值是 11010 和 10111，组合之后，第 0 位是经度的第 0 位 1，第 1 位是纬度的第 0 位 1，第 2 位是经度的第 1 位 1，第 3 位是纬度的第 1 位 0，以此类推，就能得到最终编码值 1110011101
 *   2.2 用了 GeoHash 编码后，原来无法用一个权重分数表示的一组经纬度（116.37，39.86）就可以用 1110011101 这一个值来表示，就可以保存为 Sorted Set 的权重分数了。
 *
 * 3 案例：用户打车
 *   - GEO 类型是把经纬度所在的区间编码作为 Sorted Set 中元素的权重分数，把和经纬度相关的车辆 ID 作为 Sorted Set 中元素本身的值保存下来，这样相邻经纬度的查询就可以通过编码值的大小范围查询来实现了。
 */

#if defined(__cplusplus)
extern "C" {
#endif

#define HASHISZERO(r) (!(r).bits && !(r).step)
#define RANGEISZERO(r) (!(r).max && !(r).min)
#define RANGEPISZERO(r) (r == NULL || RANGEISZERO(*r))

#define GEO_STEP_MAX 26 /* 26*2 = 52 bits. */

/* Limits from EPSG:900913 / EPSG:3785 / OSGEO:41001 */
/*维度范围*/
#define GEO_LAT_MIN -85.05112878
#define GEO_LAT_MAX 85.05112878
/*经度范围*/
#define GEO_LONG_MIN -180
#define GEO_LONG_MAX 180

typedef enum {
    GEOHASH_NORTH = 0,
    GEOHASH_EAST,
    GEOHASH_WEST,
    GEOHASH_SOUTH,
    GEOHASH_SOUTH_WEST,
    GEOHASH_SOUTH_EAST,
    GEOHASH_NORT_WEST,
    GEOHASH_NORT_EAST
} GeoDirection;

typedef struct {
    uint64_t bits;
    uint8_t step;
} GeoHashBits;

typedef struct {
    double min;
    double max;
} GeoHashRange;

typedef struct {
    GeoHashBits hash;
    GeoHashRange longitude;
    GeoHashRange latitude;
} GeoHashArea;

typedef struct {
    GeoHashBits north;
    GeoHashBits east;
    GeoHashBits west;
    GeoHashBits south;
    GeoHashBits north_east;
    GeoHashBits south_east;
    GeoHashBits north_west;
    GeoHashBits south_west;
} GeoHashNeighbors;

#define CIRCULAR_TYPE 1
#define RECTANGLE_TYPE 2
typedef struct {
    int type; /* search type */
    double xy[2]; /* search center point, xy[0]: lon, xy[1]: lat */
    double conversion; /* km: 1000 */
    double bounds[4]; /* bounds[0]: min_lon, bounds[1]: min_lat
                       * bounds[2]: max_lon, bounds[3]: max_lat */
    union {
        /* CIRCULAR_TYPE */
        double radius;
        /* RECTANGLE_TYPE */
        struct {
            double height;
            double width;
        } r;
    } t;
} GeoShape;

/*
 * 0:success
 * -1:failed
 */
void geohashGetCoordRange(GeoHashRange *long_range, GeoHashRange *lat_range);
int geohashEncode(const GeoHashRange *long_range, const GeoHashRange *lat_range,
                  double longitude, double latitude, uint8_t step,
                  GeoHashBits *hash);
int geohashEncodeType(double longitude, double latitude,
                      uint8_t step, GeoHashBits *hash);
int geohashEncodeWGS84(double longitude, double latitude, uint8_t step,
                       GeoHashBits *hash);
int geohashDecode(const GeoHashRange long_range, const GeoHashRange lat_range,
                  const GeoHashBits hash, GeoHashArea *area);
int geohashDecodeType(const GeoHashBits hash, GeoHashArea *area);
int geohashDecodeWGS84(const GeoHashBits hash, GeoHashArea *area);
int geohashDecodeAreaToLongLat(const GeoHashArea *area, double *xy);
int geohashDecodeToLongLatType(const GeoHashBits hash, double *xy);
int geohashDecodeToLongLatWGS84(const GeoHashBits hash, double *xy);
int geohashDecodeToLongLatMercator(const GeoHashBits hash, double *xy);
void geohashNeighbors(const GeoHashBits *hash, GeoHashNeighbors *neighbors);

#if defined(__cplusplus)
}
#endif
#endif /* GEOHASH_H_ */
