#include "sqlitecpp.h"


namespace SQLite
{

	namespace detail
	{
		/* callback function wrapper things */
		text_view value_get(sqlite3_value* v, text_tag)
		{
			auto txt = reinterpret_cast<const char*>(sqlite3_value_text(v));
			auto size = sqlite3_value_bytes(v);
			return text_view(txt, size);
		}

		blob_view value_get(sqlite3_value* v, blob_tag)
		{
			auto data = reinterpret_cast<const std::byte*>(sqlite3_value_blob(v));
			auto size = sqlite3_value_bytes(v);
			return blob_view(data, size);
		}

		double value_get(sqlite3_value* v, double_tag)
		{
			return sqlite3_value_double(v);
		}

		int value_get(sqlite3_value* v, int_tag)
		{
			return sqlite3_value_int(v);
		}

		sqlite_int64 value_get(sqlite3_value* v, int64_tag)
		{
			return sqlite3_value_int64(v);
		}
		
		void result_set(sqlite3_context* ctx, blob_view value, sqlite3_destructor_type d) 
		{
			sqlite3_result_blob(ctx, value.data(), static_cast<int>(value.size()), d);
		}
		void result_set(sqlite3_context* ctx, text_view value, sqlite3_destructor_type d)
		{
			sqlite3_result_text(ctx, value.data(), static_cast<int>(value.size()), d);
		}
		void result_set(sqlite3_context* ctx, double value) 
		{
			sqlite3_result_double(ctx, value);
		}
		void result_set(sqlite3_context* ctx, int value) 
		{
			sqlite3_result_int(ctx, value);
		}
		void result_set(sqlite3_context* ctx, sqlite_int64 value) 
		{
			sqlite3_result_int64(ctx, value);
		}
	}
	/* Statement */

	Statement::~Statement() noexcept
	{
		//ignore errors
		sqlite3_finalize(m_stmt);
	}

	Statement& Statement::operator=(Statement&& other)
	{
		if (this != &other) {
			finalize();
			swap(other);
		}
		return *this;
	}

	bool Statement::step()
	{
		m_data_count = 0;
		auto res = check<SQLITE_DONE, SQLITE_ROW>(sqlite3_step(m_stmt));
		m_data_count = sqlite3_data_count(m_stmt);
		return (res == SQLITE_ROW);
	}

	Statement& Statement::reset()
	{
		check_ok(sqlite3_reset(m_stmt));
		return *this;
	}

	void Statement::finalize()
	{
		check_ok(sqlite3_finalize(m_stmt));
		m_stmt = nullptr;
		m_data_count = 0;
	}

	int Statement::parameter(const char* name)
	{
		return sqlite3_bind_parameter_index(m_stmt, name);
	}

	Statement& Statement::bind(int param, text_view text, sqlite3_destructor_type dtor)
	{
		check_ok(sqlite3_bind_text(m_stmt, param, text.data(), static_cast<int>(text.size()), dtor));
		return *this;
	}

	Statement& Statement::bind(int param, blob_view blob, sqlite3_destructor_type dtor)
	{
		check_ok(sqlite3_bind_blob(m_stmt, param, blob.data(), static_cast<int>(blob.size()), dtor));
		return *this;
	}

	Statement& Statement::bind(int param, double value)
	{
		check_ok(sqlite3_bind_double(m_stmt, param, value));
		return *this;
	}

	Statement& Statement::bind(int param, int value)
	{
		check_ok(sqlite3_bind_int(m_stmt, param, value));
		return *this;
	}

	Statement& Statement::bind(int param, sqlite_int64 value)
	{
		check_ok(sqlite3_bind_int64(m_stmt, param, value));
		return *this;
	}

	text_view Statement::get(int c, text_tag)
	{
		check_column(c);
		auto txt = reinterpret_cast<const char*>(sqlite3_column_text(m_stmt, c));
		auto size = sqlite3_column_bytes(m_stmt, c);
		return text_view(txt, size);
	}

	blob_view Statement::get(int c, blob_tag)
	{
		check_column(c);
		auto data = reinterpret_cast<const std::byte*>(sqlite3_column_blob(m_stmt, c));
		auto size = sqlite3_column_bytes(m_stmt, c);
		return blob_view(data, size);
	}

	double Statement::get(int c, double_tag)
	{
		check_column(c);
		return sqlite3_column_double(m_stmt, c);
	}

	int Statement::get(int c, int_tag)
	{
		check_column(c);
		return sqlite3_column_int(m_stmt, c);
	}

	sqlite_int64 Statement::get(int c, int64_tag)
	{
		check_column(c);
		return sqlite3_column_int64(m_stmt, c);
	}

	Statement::Statement(Database& db, std::string_view stmt)
		: m_stmt(nullptr), m_data_count(0)
	{
		check_ok(sqlite3_prepare_v2(db, stmt.data(), static_cast<int>(stmt.size()), &m_stmt, nullptr));
	}

	/* Database */

	Database::Database(const std::string& filename, int flags = default_flags)
		: m_db(nullptr)
	{
		check_ok(sqlite3_open_v2(filename.c_str(), &m_db, flags, nullptr));
	}

	Database::Database(Database&& other) noexcept
		: m_db(other.m_db), m_cache(std::move(other.m_cache))
	{
		other.m_db = nullptr;
	}

	Database& Database::operator=(Database&& other) noexcept
	{
		if (&other != this) {
			sqlite3_close_v2(m_db); // no-op if m_db is null
			m_db = other.m_db;
			other.m_db = nullptr;
		}
		return *this;
	}

	/*
	Statement& Database::prepare_cached(std::string_view stmt)
	{
		auto find_it = m_cache.find(stmt);
		if (find_it != m_cache.end()) return find_it->second;
		auto [it, ok] = m_cache.emplace(stmt, Statement(*this, stmt));
		return it->second;
	}
	*/

	void Database::exec(const char* sql)
	{
		char* msg;
		auto ret = sqlite3_exec(m_db, sql, nullptr, nullptr, &msg);
		if (msg != nullptr) {
			std::string msg_str(msg);
			sqlite3_free(msg);
			throw std::runtime_error(msg_str);
		}
		check_ok(ret);
	}

	void Database::create_function(const char* name, int n_args, sql_function func, void* user_data, int text_enc)
	{
		check_ok(sqlite3_create_function(m_db, name, n_args, text_enc, user_data, &func, nullptr, nullptr));
	}
}