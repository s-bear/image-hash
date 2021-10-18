
#include "mvptable.h"
#include <algorithm>


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

void MVPTable::exec_increment_count(const std::string& table) {
	increment_count->bind("col", table);
	increment_count->reset();
	increment_count->exec();
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
		std::vector<int> shells;
		int64_t part = 0;
		sel_vps->reset();
		while (sel_vps->executeStep()) {
			auto id = sel_vps->getColumn("id").getInt64();
			int32_t bounds_1 = sel_vps->getColumn("bounds_1").getInt();
			int32_t bounds_2 = sel_vps->getColumn("bounds_2").getInt();
			int32_t bounds_3 = sel_vps->getColumn("bounds_3").getInt();
			auto vp_value = get_blob(sel_vps->getColumn("value"));

			vp_ids.push_back(id);
			uint32_t d = get_distance(vp_value, p_value);
			dists.push_back(d);
			//which shell around the vantage point does this point fall into
			// also prepare the increment_vp_count statement for incrementing the occupancy count of that shell
			increment_vp_count->bind("id", id);
			int64_t shell = -1;
			if (d >= bounds_3) {
				shell = 3;
				increment_vp_count->bind("col", "count_3");
			}
			else if (d >= bounds_2) {
				shell = 2;
				increment_vp_count->bind("col", "count_2");
			}
			else if (d >= bounds_1) {
				shell = 1;
				increment_vp_count->bind("col", "count_1");
			}
			else if (d >= 0) {
				shell = 0;
				increment_vp_count->bind("col", "count_0");
			}
			if (shell < 0) {
				throw std::runtime_error("Error inserting point: invalid distance");
			}

			//increment the count for that shell
			increment_vp_count->reset();
			increment_vp_count->exec();

			//calculate the partition
			part = 4 * part + shell; //there are at most 4 shells per vantage point
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

int64_t MVPTable::insert_item(int64_t point_id)
{
	ins_item->bind("point_id", point_id);
	ins_item->reset();
	if (ins_item->executeStep()) {
		exec_increment_count("items");
		return ins_item->getColumn(0).getInt64();
	}
	else {
		throw std::runtime_error("Error inserting item");
	}
}

int64_t MVPTable::insert_vantage_point(const blob_type& vp_value)
{
	// Inserting a vantage point:
	//   adds a new column of distances to mvp_points
	//   repartitions mvp_points (mvp_points column part)
	//   adds a new row to mvp_vantage_points

	ins_vp->bind("value", vp_value.data(), vp_value.size());
	insert_vantage_point->reset();
	if (insert_vantage_point->executeStep()) {
		increment_count->bind("col", "vantage_points");
		increment_count->reset();
		increment_count->exec();

		return insert_vantage_point->getColumn(0).getInt64();
	}
	else {
		throw std::runtime_error("Error inserting new vantage_point");
	}
}

// Add a new column to points for the given vantage_point
//  The new column is named "d{vp_id}" and will be populated by get_distance(vp_value, points.value)
// No transaction
void add_points_column_(int64_t vp_id, const point_type& vp_value)
{
	std::string col_name = "d" + std::to_string(vp_id);

	add_points_col->bind("col", col_name);
	add_points_col->bind("idx", "idx_" + col_name);
	add_points_col->reset();
	add_points_col->exec();

	// we need to compute the distance from the new vantage point to all of the existing points
	update_point->bind("col", col_name);

	get_all_point_values->reset();
	while (get_all_point_values->executeStep()) {
		auto id = get_all_point_values->getColumn(0).getInt64();
		auto p_value = get_point_value(get_all_point_values->getColumn(1));

		uint32_t d = get_distance(vp_value, p_value);
		update_point->bind("id", id);
		update_point->bind("val", d);
		update_point->reset();
		update_point->exec();
	}
}

public:
// Construct, open or create the database
Impl(const std::string& path)
	: db(path, SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE)
{
	//initialize database as necessary
	SQLite::Transaction trans(db);
	db.exec(str_init_tables);

	count_rows = make_stmt(str_count_rows);
	ins_counts = make_stmt(str_ins_counts);
	sel_vp_ids = make_stmt(str_sel_vp_ids);

	//if counts is empty, initialize it
	if (do_count_rows("mvp_counts") == 0) {
		auto num_points = do_count_rows("mvp_points");
		auto num_vantage_points = do_count_rows("mvp_vantage_points");
		auto num_items = do_count_rows("mvp_items");

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
	trans.commit();

	//precompile statements
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

	ins_item = make_stmt(str_ins_item);
	upd_item = make_stmt(str_upd_item);
	sel_item_by_id = make_stmt(str_sel_item_by_id);
}


// Insert a (point, item) pair
//  Multiple items may be associated with the same point
//  If the point is new, its distance to all existing vantage points will be computed and stored
void insert(const point_type& p_value, const item_type& item)
{
	SQLite::Transaction trans(db);
	auto point_id = insert_point_(p_value);
	insert_item_(point_id, item);
	trans.commit();
}

void add_vantage_point(const point_type& vp_value)
{
	SQLite::Transaction trans(db);

	auto vp_id = insert_vantage_point_(vp_value);
	add_points_column_(vp_id, vp_value);

	trans.commit();
}

std::vector<item_type> query(const point_type& pt, unsigned int radius, int64_t limit)
{
	//we need the distance from each vantage point to pt
	SQLite::Transaction trans(db);

	std::vector<int64_t> vp_ids;
	std::vector<uint32_t> lower_bounds, upper_bounds;
	get_vantage_points->reset();
	while (get_vantage_points->executeStep()) {
		auto id = get_vantage_points->getColumn(0).getInt64(); //vantage point id
		vp_ids.push_back(id);

		auto vp_value = get_point_value(get_vantage_points->getColumn(1)); //vantage point value
		//get the distance
		uint32_t d = get_distance(vp_value, pt);
		lower_bounds.push_back(d <= radius ? 0 : d - radius);
		upper_bounds.push_back(d >= 0xFFFFFFFF - radius ? 0xFFFFFFFF : d + radius);
	}
	update_vp_ids(vp_ids);
	//we get all of the point within the lower and upper bounds using parition_points
	// for each, we compute the distance from the query point to it, and store
	// the result in the temp.query table
	clear_query->reset();
	clear_query->exec();

	if (!partition_points) partition_points = make_partition_points(vp_ids);

	for (size_t i = 0; i < vp_ids.size(); ++i) {
		partition_points->bind(2 * i + 1, lower_bounds[i]);
		partition_points->bind(2 * i + 2, upper_bounds[i]);
	}

	partition_points->reset();
	while (partition_points->executeStep()) {
		auto id = partition_points->getColumn(0).getInt64();
		auto value = get_point_value(partition_points->getColumn(1));

		uint32_t d = get_distance(pt, value);

		insert_query->bind("id", id);
		insert_query->bind("dist", d);
		insert_query->reset();
		insert_query->exec();
	}

	std::vector<item_type> result;
	get_files_by_query->bind("limit", limit);
	get_files_by_query->reset();
	while (get_files_by_query->executeStep()) {
		result.push_back(get_item(get_files_by_query->getColumn(0)));
	}

	trans.commit();

	return result;
}

point_type find_vantage_point(size_t sample_size)
{
	SQLite::Transaction trans(db);

	// do we have any vantage points yet?
	int64_t num_vantage_points = -1;
	get_count->reset();
	if (get_count->executeStep()) {
		num_vantage_points = get_count->getColumn(1).getInt64();
	}
	else {
		throw std::runtime_error("Error reading number of vantage points");
	}

	if (num_vantage_points > 0) {
		//we need to find a point that's far from all of the existing vantage points

	}
	else {
		//we need to find a point that's far from all other points
	}

	trans.commit();
}
