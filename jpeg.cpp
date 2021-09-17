#include "imghash.h"

#include "jpeglib.h"

#include <cstdio>
#include <csetjmp>
#include <array>

using namespace imghash;

namespace {
	struct my_error_mgr {
		jpeg_error_mgr mgr;
		jmp_buf setjmp_buffer;
		std::string message;
	};

	void my_output_message(j_common_ptr cinfo)
	{
		char buffer[JMSG_LENGTH_MAX];
		(*cinfo->err->format_message)(cinfo, buffer);
		my_error_mgr* my_err = (my_error_mgr*)(cinfo->err);
		my_err->message = std::string("JPEG: ") + std::string(buffer);
	}

	void my_error_exit(j_common_ptr cinfo)
	{
		// cinfo->err points to a my_error_mgr
		my_error_mgr* my_err = (my_error_mgr*)(cinfo->err);
		// handle the error message
		(*cinfo->err->output_message)(cinfo);
		// return control to load_jpeg (at setjmp)
		longjmp(my_err->setjmp_buffer, 1);
	}
}

namespace imghash {
	bool test_jpeg(FILE* file)
	{
		unsigned char magic[2] = { 0 };
		
		auto off = ftell(file);
		fread(magic, sizeof(unsigned char), 2, file);
		fseek(file, off, SEEK_SET);

		return (magic[0] == 0xFF) && (magic[1] == 0xD8);
	}

	Image<float> load_jpeg(FILE* file, Preprocess& prep)
	{
		//1. Allocate & init decompression object
		jpeg_decompress_struct cinfo{ 0 };
		my_error_mgr jerr{ {0}, 0, {} };

		// set up error handling
		cinfo.err = jpeg_std_error(&jerr.mgr);
		jerr.mgr.error_exit = my_error_exit;
		jerr.mgr.output_message = my_output_message;
		if (setjmp(jerr.setjmp_buffer)) {
			jpeg_destroy_decompress(&cinfo);
			throw std::runtime_error(jerr.message);
		}

		jpeg_create_decompress(&cinfo);

		//2. Open source
		jpeg_stdio_src(&cinfo, file);

		//3. Read header
		jpeg_read_header(&cinfo, TRUE);

		//4. Adjust decompression settings
		cinfo.out_color_space = JCS_RGB;
		cinfo.quantize_colors = false;

		//5. Begin decompression
		jpeg_start_decompress(&cinfo);

		prep.start(cinfo.output_height, cinfo.output_width, cinfo.output_components);

		std::vector<uint8_t> scanline(cinfo.output_width * cinfo.output_components, 0);

		do {
			JSAMPROW row = (JSAMPROW)scanline.data();
			jpeg_read_scanlines(&cinfo, &row, 1);
		} while (prep.add_row(scanline.data()));

		//7. Done
		jpeg_finish_decompress(&cinfo);
		jpeg_destroy_decompress(&cinfo);

		return prep.stop();
	}
}

