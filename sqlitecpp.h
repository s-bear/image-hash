#pragma once

#include "sqlite3.h"

#include <string>
#include <string_view>
#include <stdexcept>
#include <span>
#include <unordered_map>
#include <functional>
#include <tuple>

namespace SQLite
{

	template<int... OK_FLAGS>
	inline int check(int result)
	{
		//don't do anything for non-error codes
		if (((result == OK_FLAGS) || ...)) return result;
		//it's an error
		throw std::runtime_error(sqlite3_errstr(result));
	}

	inline void check_ok(int result) { check<SQLITE_OK>(result); }

	class blob_view : private std::span<const std::byte> 
	{
	public:
		using std::span<const std::byte>::span;
		using std::span<const std::byte>::operator=;
		using std::span<const std::byte>::begin;
		using std::span<const std::byte>::end;
		using std::span<const std::byte>::size;
		using std::span<const std::byte>::data;
		using std::span<const std::byte>::empty;
		using std::span<const std::byte>::operator[];
		
		constexpr blob_view subview(size_t offset, size_t count = std::dynamic_extent) const
		{
			return std::span<const std::byte>::subspan(offset, count);
		}
	};

	using text_view = std::string_view;
	using sql_function = void(&)(sqlite3_context*, int, sqlite3_value**);

	//for tag dispatch
	class text_tag {}; class blob_tag {}; class double_tag {}; class int_tag {}; class int64_tag {};

	template<class T> struct tag_traits {};
	template<> struct tag_traits<text_tag> { using value_type = text_view; };
	template<> struct tag_traits<blob_tag> { using value_type = blob_view; };
	template<> struct tag_traits<double_tag> { using value_type = double; };
	template<> struct tag_traits<int_tag> { using value_type = int; };
	template<> struct tag_traits<int64_tag> { using value_type = sqlite_int64; };
	template<class T> using tag_value_type = tag_traits<T>::value_type;

	constexpr text_tag tag(text_view) { return {}; }
	constexpr blob_tag tag(blob_view) { return {}; }
	constexpr double_tag tag(double) { return {}; }
	constexpr int_tag tag(int) { return {}; }
	constexpr int64_tag tag(sqlite_int64) { return {}; }

	template<class T> struct value_traits { using tag_type = decltype(tag(std::declval<T>())); };
	template<> struct value_traits<text_view> { using tag_type = text_tag;};
	template<> struct value_traits<blob_view> { using tag_type = blob_tag; };
	template<> struct value_traits<double> { using tag_type = double_tag; };
	template<> struct value_traits<int> { using tag_type = int_tag; };
	template<> struct value_traits<sqlite_int64> { using tag_type = int64_tag; };
	template<class T> using value_tag_type = value_traits<T>::tag_type;

	namespace detail
	{
		/* check that each type in parameter pack <class T...> is supported by SQLite, ie. tag_type<T> exists */
		template<class AlwaysVoid, class... T>
		struct is_sqlite_type : std::false_type {};

		template<class... T>
		struct is_sqlite_type<std::void_t<value_tag_type<T>...>, T...> : std::true_type {};

		template<class... T>
		inline constexpr bool is_sqlite_type_v = is_sqlite_type<void, T...>::value;

		/* SQLite function support */

		/* get the object out of a sqlite3_value -- used with the sql_function_wrapper */
		text_view value_get(sqlite3_value* v, text_tag);
		blob_view value_get(sqlite3_value* v, blob_tag);
		double value_get(sqlite3_value* v, double_tag);
		int value_get(sqlite3_value* v, int_tag);
		sqlite_int64 value_get(sqlite3_value* v, int64_tag);

		/* set the result from sql_function_wrapper */
		void result_set(sqlite3_context*, blob_view value, sqlite3_destructor_type d = SQLITE_TRANSIENT);
		void result_set(sqlite3_context*, text_view value, sqlite3_destructor_type d = SQLITE_TRANSIENT);
		void result_set(sqlite3_context*, double value);
		void result_set(sqlite3_context*, int value);
		void result_set(sqlite3_context*, sqlite_int64 value);

		
		/* call the given function with the provided args, using value_get */
		template<class Fn, class... Tags, size_t... I>
		auto sql_invoke(Fn&& func, sqlite3_value* args[], std::tuple<Tags...>, std::index_sequence<I...>)
		{
			return std::invoke(std::forward<Fn>(func), value_get(args[I], Tags{})...);
		}

		template<class Fn, class... Tags>
		using sql_invoke_result_t = std::invoke_result_t< Fn, decltype(value_get(nullptr, Tags{}))... >;

		template<class Fn, class... Tags>
		void sql_function_wrapper(sqlite3_context* ctx, int n, sqlite3_value* args[])
		{
			if (n != sizeof...(Tags)) {
				sqlite3_result_error(ctx, "Wrong number of arguments.", -1);
				return;
			}
			void* udata = sqlite3_user_data(ctx);
			if (!udata) {
				sqlite3_result_error(ctx, "Null pointer in user_data -- should be pointer to callable!", -1);
				return;
			}
			try {
				auto& func = *static_cast<Fn*>(udata); //use static_cast for void*
				auto tags = std::tuple<Tags...>{};
				auto idxs = std::index_sequence_for<Tags...>{};
				if constexpr (std::is_void_v<sql_invoke_result_t<Fn, Tags...>>) {
					sql_invoke(func, args, tags, idxs);
				}
				else {
					result_set(ctx, sql_invoke(func, args, tags, idxs));
				}
			}
			catch (std::exception& err) {
				sqlite3_result_error(ctx, err.what(), -1);
			}
			catch (...) {
				sqlite3_result_error(ctx, "Unknown exception throws while calling user function", -1);
			}
		}

		/* check that a function can be used with create_function(), or as a callback to exec() 
		Fn must be invokable with the given Args, and each (decayed) Arg be a supported type (value_tag<std::decay_t<Arg>> exists)
		The invoke result must be either void or a supported type (value_tag<std::invoke_result_t<Fn, Args...>> exists)
		*/

		template<class AlwaysVoid, class Fn, class... Args>
		struct is_sql_function : std::false_type {};

		template<class Fn, class... Args>
		struct is_sql_function<std::void_t<sql_invoke_result_t<Fn, value_tag_type<Args>...>>, Fn, Args...>
		{
			using result_type = sql_invoke_result_t<Fn, value_tag_type<Args>...>;
			static constexpr value = std::is_void_v<result_type> || is_sqlite_type_v<std::decay_t<result_type>>;
		};

		template<class Fn, class... Args>
		inline constexpr bool is_sql_function_v = is_sql_function<void, Fn, Args...>::value;	
	}

	

	class Database;

	class Statement final
	{
		sqlite3_stmt* m_stmt;
		int m_param_count, m_data_count;

		inline void check_column(int c)
		{
			if (c >= m_data_count) throw std::runtime_error("No such column");
		}

		inline void check_param(int p)
		{
			if (p >= m_param_count) throw std::runtime_error("No such parameter");
		}

		template<class... Args, class Fn, int... columns>
		Statement& exec_impl(Fn&& func, std::integer_sequence<int, columns...>)
		{
			while (step()) std::invoke(func, get(columns, value_tag<std::decay_t<Args>>{})...);
			return *this;
		}

	public:
		constexpr Statement() noexcept : m_stmt(nullptr), m_param_count(0), m_data_count(0) {}
		~Statement() noexcept;

		//no copy
		Statement(const Statement&) = delete;
		Statement& operator=(const Statement&) = delete;

		constexpr void swap(Statement& other) noexcept
		{
			using std::swap;
			swap(m_stmt, other.m_stmt);
			swap(m_param_count, other.m_param_count);
			swap(m_data_count, other.m_data_count);
		}

		//move (in terms of swap)
		constexpr Statement(Statement&& other) noexcept : Statement()
		{
			swap(other);
		}

		Statement& operator=(Statement&& other);

		Statement(Database& db, std::string_view stmt);

		constexpr operator sqlite3_stmt* () noexcept { return m_stmt; }
		constexpr operator const sqlite3_stmt* () const noexcept { return m_stmt; }

		bool step();
		Statement& reset();
		void finalize();
		
		int parameter(const char* name);
		inline int parameter(const std::string& name) { return parameter(name.c_str()); }

		Statement& bind(int param, text_view text, sqlite3_destructor_type dtor = SQLITE_TRANSIENT);
		Statement& bind(int param, blob_view blob, sqlite3_destructor_type dtor = SQLITE_TRANSIENT);
		Statement& bind(int param, double value);
		Statement& bind(int param, int value);
		Statement& bind(int param, sqlite_int64 value);

		template<class... Args>
		Statement& bind(const char* name, Args&&... args) { return bind(parameter(name), std::forward<Args>(args)...); }
		template<class... Args>
		Statement& bind(const std::string& name, Args&&... args) { return bind(name.c_str(), std::forward<Args>(args)...); }

		text_view get(int c, text_tag);
		blob_view get(int c, blob_tag);
		double get(int c, double_tag);
		int get(int c, int_tag);
		sqlite_int64 get(int c, int64_tag);

		template<class T>
		auto get(int c = 0) { return get(c, value_tag_type<T>{}); }

		inline auto get_text(int c = 0) { return get<text_view>(c); }
		inline auto get_blob(int c = 0) { return get<blob_view>(c); }
		inline auto get_double(int c = 0) { return get<double>(c); }
		inline auto get_int(int c = 0) { return get<int>(c); }
		inline auto get_int64(int c = 0) { return get<sqlite_int64>(c); }

		/* exec(): step() until done. returns *this for chaining e.g. stmt.exec().reset(); */

		inline Statement& exec()
		{
			while (step());
			return *this;
		}

		//exec() with callback -- explicit argument types: exec<int, text_view>(func)
		template<class... Args, class Fn, std::enable_if_t<detail::is_sql_function_v<Fn,Args...>, int> = 0>
		Statement& exec(Fn&& func)
		{
			return exec_impl<Args...>(std::forward<Fn>(f), std::make_integer_sequence<int, sizeof...(Args)>{});
		}

		//exec with callback -- deduced argument types from std::function
		template<class... Args, class Ret>
		Statement& exec(std::function<Ret(Args...)> func)
		{
			static_assert(detail::is_sql_function_v<decltype(func), Args...>, "Invalid exec() callback signature");
			return exec_impl<Args...>(std::move(func), std::make_integer_sequence<int, sizeof...(Args)>{});
		}

		//exec with callback -- forward to std::function version to deduce argument & return types
		template<class Fn, std::enable_if_t<!detail::is_sql_function_v<Fn>, int> = 0> 
		Statement& exec(Fn&& func)
		{
			return exec(std::function(std::forward<Fn>(func)));
		}

		template<class T>
		auto step_get(int c = 0)
		{
			//if step() returns false it means that iteration is complete
			// maybe that shouldn't throw an exception?
			if (step()) return get<T>(c);
			else throw std::runtime_error("Statement did not produce a value");
		}

		inline auto step_get_text(int c = 0) { return step_get<text_view>(c); }
		inline auto step_get_blob(int c = 0) { return step_get<blob_view>(c); }
		inline auto step_get_double(int c = 0) { return step_get<double>(c); }
		inline auto step_get_int(int c = 0) { return step_get<int>(c); }
		inline auto step_get_int64(int c = 0) { return step_get<sqlite_int64>(c); }	
	};

	constexpr void swap(Statement& a, Statement& b) { a.swap(b); }

	class Database final
	{
		sqlite3* m_db;
		struct hash_type : std::hash<std::string_view>
		{
			using is_transparent = void; //type doesn't matter, this just enables us to use std::string & std::string_view
		};
		struct equal_type : std::equal_to<void> {};
		std::unordered_map<std::string, Statement, hash_type, equal_type> m_cache;

	public:
		static constexpr int default_flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;

		constexpr Database() noexcept : m_db(nullptr) {}

		Database(Database&& other) noexcept;
		Database& operator=(Database&& other) noexcept;
		
		Database(const Database&) = delete;
		Database& operator=(const Database&) = delete;

		Database(const std::string& filename, int flags = default_flags);
		
		constexpr operator sqlite3* () noexcept { return m_db; }
		constexpr operator const sqlite3* () const noexcept { return m_db; }

		inline [[nodiscard]] Statement prepare(std::string_view stmt) { return Statement(*this, stmt); }
		inline [[nodiscard]] Statement operator()(std::string_view stmt) { return prepare(stmt); }

		//Statement& prepare_cached(std::string_view stmt);
		//inline Statement& operator[](std::string_view stmt) { return prepare_cached(stmt); }

		void exec(const char* sql);
		inline void exec(const std::string& sql) { exec(sql.c_str()); }

		void create_function(const char* name, int n_args, sql_function func, void* user_data = nullptr, int text_enc = SQLITE_UTF8);

		template<class R, class... Args>
		void create_function(const char* name, R(&func)(Args...), int text_enc = SQLITE_UTF8)
		{
			create_function(name, static_cast<int>(sizeof...(Args)), detail::sql_function_wrapper<decltype(func), value_tag_type<Args>...>, &func, text_enc);
		}
	};
}

