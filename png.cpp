#include "imghash.h"

#include "png.h"

namespace imghash {

	bool test_png(FILE* file)
	{
		png_byte header[8] = { 0 };
		auto off = ftell(file);
		fread(header, sizeof(png_byte), 8, file);
		fseek(file, off, SEEK_SET);
		return png_sig_cmp(header, 0, 8) == 0;
	}

	namespace {
		void my_error_fn(png_structp png_ptr, png_const_charp error_msg) 
		{
			std::string* msg = (std::string*)png_get_error_ptr(png_ptr);
			*msg = error_msg;
			longjmp(png_jmpbuf(png_ptr), 1);
		}
		void my_warning_fn(png_structp png_ptr, png_const_charp warning_msg) 
		{
			return; //ignore warnings
		}
	}

	Image<float> load_png(FILE* file, Preprocess& prep)
	{
		std::string error_message;
		png_structp png_ptr = png_create_read_struct(PNG_LIBPNG_VER_STRING,
			&error_message, my_error_fn, my_warning_fn);
		if (!png_ptr) 
		{
			throw std::runtime_error("PNG: Error creating read struct");
		}
		png_infop info_ptr = png_create_info_struct(png_ptr);
		if (!info_ptr)
		{
			png_destroy_read_struct(&png_ptr, nullptr, nullptr);
			throw std::runtime_error("PNG: Error creating info struct");
		}
		png_infop end_info = png_create_info_struct(png_ptr);
		if (!info_ptr)
		{
			png_destroy_read_struct(&png_ptr, &info_ptr, nullptr);
			throw std::runtime_error("PNG: Error creating info struct");
		}
		if (setjmp(png_jmpbuf(png_ptr))) {
			png_destroy_read_struct(&png_ptr, &info_ptr, &end_info);	
			throw std::runtime_error("PNG: Error");
		}

		png_init_io(png_ptr, file);
		//ignore all unkown chunks
		png_set_keep_unknown_chunks(png_ptr, PNG_HANDLE_CHUNK_NEVER, nullptr, 0);
		
		png_read_info(png_ptr, info_ptr);
		
		png_uint_32 width = png_get_image_width(png_ptr, info_ptr);
		png_uint_32 height = png_get_image_height(png_ptr, info_ptr);
		png_byte channels = png_get_channels(png_ptr, info_ptr);
		int color_type = png_get_color_type(png_ptr, info_ptr);
		int bit_depth = png_get_bit_depth(png_ptr, info_ptr);
		int interlace = png_get_interlace_type(png_ptr, info_ptr);
		//we want an RGB image with no alpha
		if (color_type == PNG_COLOR_TYPE_PALETTE) {
			png_set_palette_to_rgb(png_ptr);
		}
		if (color_type == PNG_COLOR_TYPE_GRAY 
			|| color_type == PNG_COLOR_TYPE_GRAY_ALPHA) {
			png_set_gray_to_rgb(png_ptr);
			
			if (bit_depth < 8) {
				png_set_expand_gray_1_2_4_to_8(png_ptr);
			}
		}
		if (bit_depth == 16) {
			png_set_strip_16(png_ptr);
		}
		
		png_color_16 bg = { 0 };
		png_color_16p bg_ptr;
		if (png_get_bKGD(png_ptr, info_ptr, &bg_ptr)) {
			png_set_background(png_ptr, bg_ptr, PNG_BACKGROUND_GAMMA_FILE, 1, 1.0);
		}
		else {
			png_set_background(png_ptr, &bg, PNG_BACKGROUND_GAMMA_FILE, 0, 1.0);
		}
		/*
		double gamma;
		if (!png_get_gAMA(png_ptr, info_ptr, &gamma)) gamma = 0.4545;
		png_set_gamma(png_ptr, 0.4545, gamma);
		*/

		png_read_update_info(png_ptr, info_ptr);
		
		channels = png_get_channels(png_ptr, info_ptr);
		size_t rowbytes = png_get_rowbytes(png_ptr, info_ptr);

		if (interlace == PNG_INTERLACE_NONE) {
			//we can read row-by-row
			std::vector<uint8_t> row(rowbytes, 0);
			prep.start(height, width, channels);
			do {
				png_read_row(png_ptr, row.data(), nullptr);
			} while (prep.add_row(row.data()));
			return prep.stop();
		}
		else {
			//interlaced.. let libpng handle it
			Image<uint8_t> img(height, width, channels, rowbytes*height, rowbytes);
			std::vector<uint8_t*> rows;
			rows.reserve(height);
			for (size_t y = 0; y < height; ++y) rows.push_back(img.data.get() + img.index(y,0,0));
			png_read_image(png_ptr, rows.data());
			
			return prep.apply(img);
		}
	}
}
