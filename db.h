
#pragma once
#include "imghash.h"

#include <cstdint>
#include <string>
#include <vector>
#include <memory>

namespace imghash {

	class Database {
		class Impl;
		std::unique_ptr<Impl> impl;
	public:
		
		using point_type = Hash::hash_type;

		//Open the database
		Database(const std::string& path);

		//Close the database
		~Database();

		//Add a file
		void insert(const point_type& hash, const std::string& name);

		//Find similar images
		void query(const point_type& hash, unsigned int dist);

		//Add a vantage point for querying
		// NB this is expensive: it computes (and stores) the distance from the new
		//  vantage point to every point already in the database
		void add_vantage_point(const point_type& hash);

		//Find a point that would make a good vantage point
		// this is a point that is far from all existing vantage points
		// if there are no vantage points yet, the best is a point that is distant from most other points
		//  this will be determined by randomly subsampling 'sample_size' candidate vantage points and computing 
		//  the distance from each to another random subsample of "friends". The candidate with the most distant
		//  friends will be returned. That is, each candidate's friends will be sorted from nearest to furthest
		//  and ranked relative to the previous best candidate. The candidate with more friends that are further
		//  away than the other wins.
		//  NB that this algorithm is expensive:
		//    O(sample_size*sample_size) distance computations
		//    + O(sample_size*sample_size*sample_size*log(sample_size)) distance comparisons for the ranking
		point_type find_vantage_point(size_t sample_size = 25);
	};
}
