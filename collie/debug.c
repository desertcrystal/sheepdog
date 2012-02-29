#include "collie.h"

struct debug_cmd_data {
	int t_enable;
	int t_cat;
} debug_cmd_data;

static void print_trace_item(struct trace_graph_item *item)
{
	int i;

	if (item->type == TRACE_GRAPH_ENTRY) {
		printf("             |  ");
		for (i = 0; i < item->depth; i++)
			printf("   ");
		printf("%s() {\n", item->fname);
	} else {
		unsigned duration = item->return_time - item->entry_time;
		unsigned quot = duration / 1000, rem = duration % 1000;
		printf("%8u.%-3u |  ", quot, rem);
		for (i = 0; i < item->depth; i++)
			printf("   ");
		printf("}\n");
	}
}

static void parse_trace_buffer(char *buf, int size)
{
	struct trace_graph_item *item = (struct trace_graph_item *)buf;
	int sz = size / sizeof(struct trace_graph_item), i;

	printf("   Time(us)  |  Function Graph\n");
	printf("-------------------------------\n");
	for (i = 0; i < sz; i++)
		print_trace_item(item++);
	return;
}

static int do_trace_cat(void)
{
	int fd, ret;
	struct sd_req hdr;
	struct sd_rsp *rsp = (struct sd_rsp *)&hdr;
	unsigned rlen, wlen;
	char *buf = xzalloc(TRACE_BUF_LEN * 12);

	fd = connect_to(sdhost, sdport);
	if (fd < 0)
		return EXIT_SYSFAIL;

	memset(&hdr, 0, sizeof(hdr));
	hdr.opcode = SD_OP_TRACE_CAT;
	hdr.data_length = rlen = TRACE_BUF_LEN;
	hdr.epoch = node_list_version;

	wlen = 0;
	ret = exec_req(fd, &hdr, buf, &wlen, &rlen);
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

	parse_trace_buffer(buf, rlen);

	free(buf);
	return EXIT_SUCCESS;
}

static int debug_trace(int argc, char **argv)
{
	int fd, ret;
	struct sd_req hdr;
	struct sd_rsp *rsp = (struct sd_rsp *)&hdr;
	unsigned rlen, wlen;

	if (debug_cmd_data.t_cat)
		return do_trace_cat();

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
		case 'C':
			debug_cmd_data.t_cat = 1;
			break;
	}

	return 0;
}

static struct subcommand debug_cmd[] = {
	{"trace", NULL, "etCaprh", "debug the cluster",
		0, debug_trace},
	{NULL,},
};

struct command debug_command = {
	"debug",
	debug_cmd,
	debug_parser
};
