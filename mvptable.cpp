
#include "mvptable.h"
#include <algorithm>


// Construct, open or create the database
MVPTable::MVPTable(std::shared_ptr<SQLite::Database> db) 
	: db(db)
{
	//initialize database as necessary
	db->exec(str_init_tables);

	count_rows = make_stmt(str_count_rows);
	ins_counts = make_stmt(str_ins_counts);
	sel_vp_ids = make_stmt(str_sel_vp_ids);
	sel_count = make_stmt(str_sel_count);

	//if counts is empty, initialize it
	if (exec_count_rows("mvp_counts") == 0) {
		auto num_points = exec_count_rows("mvp_points");
		auto num_vantage_points = exec_count_rows("mvp_vantage_points");
		auto num_items = exec_count_rows("mvp_items");

		ins_counts->bind("points", num_points);
		ins_counts->bind("vantage_points", num_vantage_points);
		ins_counts->bind("items", num_items);
		ins_counts->reset();
		ins_counts->exec();
	}

	//get all of the vantage point ids
	std::vector<int64_t> vp_ids;
	sel_vp_ids->reset();
	while (sel_vp_ids->executeStep()) {
		vp_ids.push_back(sel_vp_ids->getColumn(0).getInt64());
	}
	vp_ids_ = std::move(vp_ids);

	//precompile more statements
	increment_count = make_stmt(str_increment_count);

	ins_point = make_stmt(str_ins_point(vp_ids_));
	sel_all_points = make_stmt(str_sel_all_points);
	sel_point_by_value = make_stmt(str_sel_point_by_value);
	upd_point = make_stmt(str_upd_point);
	add_points_col = make_stmt(str_add_points_col);

	sel_vps = make_stmt(str_sel_vps);
	ins_vp = make_stmt(str_ins_vp);
	increment_vp_count = make_stmt(str_increment_vp_count);

	del_query = make_stmt(str_del_query);
	ins_query = make_stmt(str_ins_query);
}

const std::string MVPTable::str_ins_point(const std::vector<int64_t>& vp_ids) {
	std::string stmt1 = "INSERT INTO mvp_points(part, value";
	std::string stmt2 = ") VALUES ($part, $value";
	for (int64_t id : vp_ids) {
		auto id_str = std::to_string(id);
		stmt1 += ", d" + id_str;
		stmt2 += ", $d" + id_str;
	}
	return stmt1 + stmt2 + ") RETURNING id;";
}

MVPTable::blob_type MVPTable::get_blob(SQLite::Column& col) {
	size_t n = col.getBytes();
	const uint8_t* data = static_cast<const uint8_t*>(col.getBlob());
	return blob_type(data, data + n);
}

MVPTable::stmt_ptr MVPTable::make_stmt(const std::string stmt)
{
	if (db == nullptr) throw std::runtime_error("No database connection");
	return std::make_unique<SQLite::Statement>(*db, stmt);
}

void MVPTable::update_vp_ids(const std::vector<int64_t>& vp_ids)
{
	if (!std::equal(vp_ids_.begin(), vp_ids_.end(), vp_ids.begin(), vp_ids.end())) {
		vp_ids_ = vp_ids;
		ins_point.reset();
	}
}

int64_t MVPTable::exec_count_rows(const std::string& table) {
	count_rows->bind("table", table);
	count_rows->reset();
	if (count_rows->executeStep()) {
		return count_rows->getColumn(0).getInt64();
	}
	else {
		throw std::runtime_error("Error counting rows of table: " + table);
	}
}

int64_t MVPTable::exec_sel_count(const std::string& table) {
	sel_count->bind("col", table);
	sel_count->reset();
	if (sel_count->executeStep()) {
		return sel_count->getColumn(0).getInt64();
	}
	else {
		throw std::runtime_error("Error getting cached row count: " + table);
	}
}

void MVPTable::exec_increment_count(const std::string& table) {
	increment_count->bind("col", table);
	increment_count->reset();
	increment_count->exec();
}

namespace {
	inline bool in_bounds(int32_t d, int32_t radius, int32_t low)
	{
		// ( [ )  or [ ( )
		return d + radius >= low; //TODO: overflow
	}
	inline bool in_bounds(int32_t d, int32_t radius, int32_t low, int32_t high)
	{
		return d + radius >= low && d - radius < high;
	}
}

int64_t MVPTable::insert_point(const blob_type& p_value)
{
	//is the point already in the database?
	sel_point_by_value->bind("value", p_value.data(), p_value.size());
	sel_point_by_value->reset();
	if (sel_point_by_value->executeStep()) {
		//yes
		return sel_point_by_value->getColumn(0).getInt64();
	}
	else {
		//no: we need to add the point

		//iterate over the vantage points, calculating the distance from each to p_value
		// and calculating which parition p_value falls into
		std::vector<int64_t> vp_ids;
		std::vector<int32_t> dists;
		int64_t part = 0;
		sel_vps->reset();
		while (sel_vps->executeStep()) {
			auto id = sel_vps->getColumn("id").getInt64();
			int32_t bounds_1 = sel_vps->getColumn("bounds_1").getInt();
			int32_t bounds_2 = sel_vps->getColumn("bounds_2").getInt();
			int32_t bounds_3 = sel_vps->getColumn("bounds_3").getInt();
			auto vp_value = get_blob(sel_vps->getColumn("value"));

			vp_ids.push_back(id);
			uint32_t d = distance(vp_value, p_value);
			dists.push_back(d);
			//which shell around the vantage point does this point fall into
			// also prepare the increment_vp_count statement for incrementing the occupancy count of that shell
			increment_vp_count->bind("id", id);
			int64_t shell = -1;
			if (in_bounds(d, 0, bounds_3)) {
				shell = 3;
				increment_vp_count->bind("col", "count_3");
			}
			else if (in_bounds(d, 0, bounds_2, bounds_3)) {
				shell = 2;
				increment_vp_count->bind("col", "count_2");
			}
			else if (in_bounds(d, 0, bounds_1, bounds_2)) {
				shell = 1;
				increment_vp_count->bind("col", "count_1");
			}
			else if (in_bounds(d, 0, 0, bounds_1)) {
				shell = 0;
				increment_vp_count->bind("col", "count_0");
			}
			else {
				throw std::runtime_error("Error inserting point: invalid distance");
			}

			//increment the count for that shell
			increment_vp_count->reset();
			increment_vp_count->exec();

			//calculate the partition
			//each vp has 4 shells, so we index by multiples of 4 based on the vp's id
			// that is, each vp gets [4*id, 4*id + 3] to store its shell
			// we index by shell in reverse order because by default (before partitioning) all of the
			// points fall in shell 3, so this way adding a new vantage point doesn't require repartitioning
			part += (3-shell)*4*id;
		}

		//update the insert_point statement if vp_ids changed
		if (!ins_point) ins_point = make_stmt(str_ins_point(vp_ids_));

		ins_point->bind("part", part);
		ins_point->bind("value", p_value.data(), p_value.size());
		for (size_t i = 0; i < dists.size(); ++i) {
			ins_point->bind(i + 3, dists[i]); //the first parameter has index 1, so these start at 3
		}
		ins_point->reset();
		if (ins_point->executeStep()) {
			exec_increment_count("points");

			return ins_point->getColumn(0).getInt64();
		}
		else {
			throw std::runtime_error("Error inserting point");
		}
	}
}

int64_t MVPTable::insert_vantage_point(const blob_type& vp_value)
{
	// Inserting a vantage point:
	//   adds a new row to mvp_vantage_points
	//   adds a new column of distances to mvp_points
	//   calculate the distance from the vantage point to each point

	//1. add the new row to mvp_vantage_points and get the new vp's id
	// by default, the bounds are all 0, so all of the points will fall into shell 3
	// so count_3 needs to be set to the total number of points
	// the way parition indexing works, adding the new vantage point does not require
	// updating any existing partitions
	int64_t point_count = exec_sel_count("points");

	int64_t vp_id;
	ins_vp->bind("count_3", point_count);
	ins_vp->bind("value", vp_value.data(), vp_value.size());
	ins_vp->reset();
	if (ins_vp->executeStep()) {
		exec_increment_count("vantage_points");
		vp_id = ins_vp->getColumn("id").getInt64();
	}
	else {
		throw std::runtime_error("Error inserting new vantage_point");
	}

	//2. add the new column of distances to mvp_points

	std::string col_name = "d" + std::to_string(vp_id);

	add_points_col->bind("col", col_name);
	add_points_col->bind("idx", "mvp_idx_" + col_name);
	add_points_col->reset();
	add_points_col->exec();

	//3. compute the distance from the new vantage point to all of the existing points (and store)
	upd_point->bind("col", col_name);

	sel_all_points->reset();
	while (sel_all_points->executeStep()) {
		auto id = sel_all_points->getColumn("id").getInt64();
		auto p_value = get_blob(sel_all_points->getColumn("value"));

		uint32_t d = distance(vp_value, p_value);
		upd_point->bind("id", id);
		upd_point->bind("value", d);
		upd_point->reset();
		upd_point->exec();
	}
}

std::vector<int64_t> MVPTable::query(const blob_type& q_value, uint32_t radius, int64_t limit)
{
	// Get the distance from the query point to each vantage point
	// and get the parititions that the query covers
	std::vector<int64_t> vp_ids;
	std::vector<int32_t> dists;
	std::vector<int64_t> parts;
	parts.push_back(0);
	sel_vps->reset();
	while (sel_vps->executeStep()) {
		auto id = sel_vps->getColumn("id").getInt64();
		int32_t bounds_1 = sel_vps->getColumn("bounds_1").getInt();
		int32_t bounds_2 = sel_vps->getColumn("bounds_2").getInt();
		int32_t bounds_3 = sel_vps->getColumn("bounds_3").getInt();
		auto vp_value = get_blob(sel_vps->getColumn("value"));

		vp_ids.push_back(id);
		uint32_t d = distance(vp_value, q_value);
		dists.push_back(d);

		std::vector<int> shells;
		if (in_bounds(d, 0, bounds_3)) {
			shells.push_back(3);
		}
		else if (in_bounds(d, 0, bounds_2, bounds_3)) {
			shells.push_back(2);
		}
		else if (in_bounds(d, 0, bounds_1, bounds_2)) {
			shells.push_back(1);
		}
		else if (in_bounds(d, 0, 0, bounds_1)) {
			shells.push_back(0);
		}
		else {
			throw std::runtime_error("Error inserting point: invalid distance");
		}

		if (shells.size() == 1) {
			//a single shell -- we can modify the existing partitions in place
			for (auto& p : parts) {
				p += (3 - shells[0]) * 4 * id;
			}
		}
		else {
			//multple shells -- the number of partitions the query covers will grow
			std::vector<int64_t> new_parts;
			new_parts.reserve(parts.size() * shells.size());
			for (auto p : parts) {
				for (auto s : shells) {
					new_parts.push_back(p + (3 - s) * 4 * id);
				}
			}
			parts = std::move(new_parts);
		}
	}
	
	update_vp_ids(vp_ids);

	// get all of the points in the covered partitions
	// then we can evaluate the distance from the remaining points to the query point

	// TODO
	

	return result;
}

MVPTable::blob_type MVPTable::find_vantage_point(size_t sample_size)
{
	// do we have any vantage points yet?
	int64_t num_vantage_points = exec_sel_count("vantage_points");

	if (num_vantage_points > 0) {
		//we need to find a point that's far from all of the existing vantage points

	}
	else {
		//we need to find a point that's far from all other points
	}
}
