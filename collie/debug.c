#include "collie.h"

struct debug_cmd_data {
	int t_enable;
} debug_cmd_data;

static int debug_trace(int argc, char **argv)
{
	int fd, ret;
	struct sd_req hdr;
	struct sd_rsp *rsp = (struct sd_rsp *)&hdr;
	unsigned rlen, wlen;

	fd = connect_to(sdhost, sdport);
	if (fd < 0)
		return EXIT_SYSFAIL;

	memset(&hdr, 0, sizeof(hdr));

	hdr.opcode = SD_OP_TRACE;
	hdr.epoch = node_list_version;
	hdr.data_length = debug_cmd_data.t_enable;

	rlen = 0;
	wlen = 0;
	ret = exec_req(fd, &hdr, NULL, &wlen, &rlen);
	close(fd);

	if (ret) {
		fprintf(stderr, "Failed to connect\n");
		return EXIT_SYSFAIL;
	}

	if (rsp->result != SD_RES_SUCCESS) {
		fprintf(stderr, "Trace failed: %s\n",
				sd_strerror(rsp->result));
		return EXIT_FAILURE;
	}

	return EXIT_SUCCESS;
}

static int debug_parser(int ch, char *opt)
{
	switch (ch) {
		case 'e':
			debug_cmd_data.t_enable = 1;
			break;
		case 't':
			debug_cmd_data.t_enable = 0;
			break;
	}

	return 0;
}

static struct subcommand debug_cmd[] = {
	{"trace", NULL, "etaprh", "debug the cluster",
		0, debug_trace},
	{NULL,},
};

struct command debug_command = {
	"debug",
	debug_cmd,
	debug_parser
};
