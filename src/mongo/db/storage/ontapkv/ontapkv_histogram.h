
#ifndef UTIL_HISTOGRAM_HEADER
#define UTIL_HISTOGRAM_HEADER

#include <string>
#include <stdint.h>
#include "mongo/bson/bsonobjbuilder.h"

namespace mongo {

    /**
     * A histogram for a 32-bit integer range.
     */
    class Histogram {
    public:
        /**
         * Construct a histogram with 'numBuckets' buckets, optionally
         * having the first bucket start at 'initialValue' rather than
         * 0. By default, the histogram buckets will be 'bucketSize' wide.
         *
         * Usage example:
         *   Histogram::Options opts;
         *   opts.numBuckets = 3;
         *   opts.bucketSize = 10;
         *   Histogram h( opts );
         *
         *   Generates the bucket ranges [0..10],[11..20],[21..max_int]
         *
         * Alternatively, the flag 'exponential' could be turned on, in
         * which case a bucket's maximum value will be
         *    initialValue + bucketSize * 2 ^ [0..numBuckets-1]
         *
         * Usage example:
         *   Histogram::Options opts;
         *   opts.numBuckets = 4;
         *   opts.bucketSize = 125;
         *   opts.exponential = true;
         *   Histogram h( opts );
         *
         *   Generates the bucket ranges [0..125],[126..250],[251..500],[501..max_int]
         */
        struct Options {
            uint32_t numBuckets;
            uint32_t bucketSize;
            uint32_t initialValue;

            // use exponential buckets?
            bool            exponential;

            Options()
                : numBuckets(0)
                , bucketSize(0)
                , initialValue(0)
                , exponential(false) {}
        };
        Histogram( const Options& opts );
        ~Histogram();

        /**
         * Find the bucket that 'element' falls into and increment its count.
         */
        void insert( uint32_t element );

        /**
         * Render the histogram as string that can be used inside an
         * HTML doc.
         */
        std::string toHTML() const;

        // testing interface below -- consider it private

        /**
         * Return the count for the 'bucket'-th bucket.
         */
        uint64_t getCount( uint32_t bucket ) const;

        /**
         * Return the maximum element that would fall in the
         * 'bucket'-th bucket.
         */
        uint32_t getBoundary( uint32_t bucket ) const;

        /**
         * Return the number of buckets in this histogram.
         */
        uint32_t getBucketsNum() const;

	/**
	 * Prints Histogram.
	 */
	void printHistogram();

	/**
	 * Appends histogram data into stats
	 */
	void appendHistogramStats(BSONObjBuilder *bob);

    private:
        /**
         * Returns the bucket where 'element' should fall
         * into. Currently assumes that 'element' is greater than the
         * minimum 'inialValue'.
         */
        uint32_t _findBucket( uint32_t element ) const;

        uint32_t  _initialValue;  // no value lower than it is recorded
        uint32_t  _numBuckets;    // total buckets in the histogram

        // all below owned here
        uint32_t* _boundaries;    // maximum element of each bucket
        uint64_t* _buckets;       // current count of each bucket

        //Histogram( const Histogram& );
        //Histogram& operator=( const Histogram& );
    };

}  // namespace mongo
#endif
