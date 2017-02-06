#include "ontapkv_histogram.h"

#include <iomanip>
#include <limits>
#include <sstream>
#include <iostream>

namespace mongo {

    using std::ostringstream;
    using std::setfill;
    using std::setw;

    Histogram::Histogram( const Options& opts )
        : _initialValue( opts.initialValue )
        , _numBuckets( opts.numBuckets )
        , _boundaries( new uint32_t[_numBuckets] )
        , _buckets( new uint64_t[_numBuckets] ) {

        // TODO more sanity checks
        // + not too few buckets
        // + initialBucket and bucketSize fit within 32 bit ints

        // _boundaries store the maximum value falling in that bucket.
        if ( opts.exponential ) {
            uint32_t twoPow = 1; // 2^0
            for ( uint32_t i = 0; i < _numBuckets - 1; i++) {
                _boundaries[i] = _initialValue + opts.bucketSize * twoPow;
                twoPow *= 2;     // 2^i+1
            }
        }
        else {
            _boundaries[0] = _initialValue + opts.bucketSize;
            for ( uint32_t i = 1; i < _numBuckets - 1; i++ ) {
                _boundaries[i] = _boundaries[ i-1 ] + opts.bucketSize;
            }
        }
        _boundaries[ _numBuckets-1 ] = std::numeric_limits<uint32_t>::max();

        for ( uint32_t i = 0; i < _numBuckets; i++ ) {
            _buckets[i] = 0;
        }
    }

    Histogram::~Histogram() {
        delete [] _boundaries;
        delete [] _buckets;
    }

    void Histogram::insert( uint32_t element ) {
        if ( element < _initialValue) return;
        if ( element > _boundaries[_numBuckets - 1]) return;

        _buckets[ _findBucket(element) ] += 1;
    }

    std::string Histogram::toHTML() const {
        uint64_t max = 0;
        for ( uint32_t i = 0; i < _numBuckets; i++ ) {
            if ( _buckets[i] > max ) {
                max = _buckets[i];
            }
        }
        if ( max == 0 ) {
            return "histogram is empty\n";
        }

        // normalize buckets to max
        const int maxBar = 20;
        ostringstream ss;
        for ( uint32_t i = 0; i < _numBuckets; i++ ) {
            int barSize = _buckets[i] * maxBar / max;
            ss << std::string( barSize,'*' )
               << setfill(' ') << setw( maxBar-barSize + 12 )
               << _boundaries[i] << '\n';
        }

        return ss.str();
    }

    uint64_t Histogram::getCount( uint32_t bucket ) const {
        if ( bucket >= _numBuckets ) return 0;

        return _buckets[ bucket ];
    }

    uint32_t Histogram::getBoundary( uint32_t bucket ) const {
        if ( bucket >= _numBuckets ) return 0;

        return _boundaries[ bucket ];
    }

    uint32_t Histogram::getBucketsNum() const {
        return _numBuckets;
    }

    uint32_t Histogram::_findBucket( uint32_t element ) const {
        // TODO assert not too small a value?

        uint32_t low = 0;
        uint32_t high = _numBuckets - 1;
        while ( low < high ) {
            // low + ( (high - low) / 2 );
            uint32_t mid = ( low + high ) >> 1;
            if ( element > _boundaries[ mid ] ) {
                low = mid + 1;
            }
            else {
                high = mid;
            }
        }
        return low;
    }

    void Histogram::printHistogram() {
	std::cout << "*******Range********" << "   Count   " << std::endl; 
	int lower_range = _initialValue;
	for ( uint32_t i = 0; i < _numBuckets; i++ ) {
		std::cout << lower_range << " - " << _boundaries[i] << " : " << _buckets[i] << std::endl;
		lower_range = _boundaries[i];
	} 
    }

    void Histogram::appendHistogramStats(BSONObjBuilder *bob) {
	int lower_range = _initialValue;
	for ( uint32_t i = 0; i < _numBuckets; i++ ) {
		std::ostringstream desc;
		desc << lower_range << " - " << _boundaries[i];
		bob->appendNumber(desc.str(), _buckets[i]);
		lower_range = _boundaries[i];
	} 
    }
	
}  // namespace mongo
