/*
 * Copyright (C) 2011 Taobao Inc.
 *
 * Liu Yuan <namei.unix@gmail.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License version
 * 2 as published by the Free Software Foundation.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>

#include "sheep_priv.h"
#include "strbuf.h"
#include "util.h"


extern char *obj_path;

extern mode_t def_fmode;

static int def_store_flags = O_DSYNC | O_RDWR;

static int simple_store_write(uint64_t oid, struct siocb *iocb);

static int simple_store_init(char *path)
{
	int epoch, latest_epoch;
	DIR *dir;
	struct dirent *dent;
	char p[PATH_MAX];

	eprintf("use simple store driver\n");
	strcpy(p, path);
	latest_epoch = get_latest_epoch();
	for (epoch = 1; epoch <= latest_epoch; epoch++) {
		snprintf(p, PATH_MAX, "%s/%08u", path, epoch);
		dir = opendir(p);
		if (!dir) {
			if (errno == ENOENT)
				continue;

			vprintf(SDOG_ERR, "failed to open the epoch directory: %m\n");
			return SD_RES_EIO;
		}

		vprintf(SDOG_INFO, "found the object directory %s\n", path);
		while ((dent = readdir(dir))) {
			uint64_t oid;

			if (!strcmp(dent->d_name, ".") ||
					!strcmp(dent->d_name, ".."))
				continue;

			oid = strtoull(dent->d_name, NULL, 16);
			if (oid == 0 || oid == ULLONG_MAX)
				continue;

			if (!is_vdi_obj(oid))
				continue;

			vprintf(SDOG_DEBUG, "found the VDI object %" PRIx64 "\n", oid);

			set_bit(oid_to_vid(oid), sys->vdi_inuse);
		}
		closedir(dir);
	}
	return SD_RES_SUCCESS;
}

static int write_last_sector(int fd)
{
	const int size = SECTOR_SIZE;
	char *buf;
	int ret;
	off_t off = SD_DATA_OBJ_SIZE - size;

	buf = valloc(size);
	if (!buf) {
		eprintf("failed to allocate memory\n");
		return SD_RES_NO_MEM;
	}
	memset(buf, 0, size);

	ret = xpwrite(fd, buf, size, off);
	if (ret != size)
		ret = SD_RES_EIO;
	else
		ret = SD_RES_SUCCESS;
	free(buf);

	return ret;
}

static int err_to_sderr(uint64_t oid, int err)
{
	int ret;
	if (err == ENOENT) {
		struct stat s;

		if (stat(obj_path, &s) < 0) {
			eprintf("corrupted\n");
			ret = SD_RES_EIO;
		} else {
			dprintf("object %016" PRIx64 " not found locally\n", oid);
			ret = SD_RES_NO_OBJ;
		}
	} else {
		eprintf("%m\n");
		ret = SD_RES_UNKNOWN;
	}
	return ret;
}

/*
 * Preallocate the whole object to get a better filesystem layout.
 */
static int prealloc(int fd)
{
	int ret = fallocate(fd, 0, 0, SD_DATA_OBJ_SIZE);
	if (ret < 0) {
		if (errno != ENOSYS && errno != EOPNOTSUPP)
			ret = SD_RES_SYSTEM_ERROR;
		else
			ret = write_last_sector(fd);
	} else
		ret = SD_RES_SUCCESS;
	return ret;
}

static int simple_store_open(uint64_t oid, struct siocb *iocb, int create)
{
	struct strbuf buf = STRBUF_INIT;
	int ret = SD_RES_SUCCESS, fd;
	int flags = def_store_flags;

	if (sys->use_directio && is_data_obj(oid))
		flags |= O_DIRECT;

	if (create)
		flags |= O_CREAT | O_TRUNC;

	strbuf_addstr(&buf, obj_path);
	strbuf_addf(&buf, "%08u/%016" PRIx64, iocb->epoch, oid);
	fd = open(buf.buf, flags, def_fmode);
	if (fd < 0) {
		ret = err_to_sderr(oid, errno);
		goto out;
	}
	iocb->fd = fd;
	ret = SD_RES_SUCCESS;
	if (!(iocb->flags & SD_FLAG_CMD_COW) && create) {
		ret = prealloc(fd);
		if (ret != SD_RES_SUCCESS)
			close(fd);
	}
out:
	strbuf_release(&buf);
	return ret;
}

static int simple_store_write(uint64_t oid, struct siocb *iocb)
{
	int size = xpwrite(iocb->fd, iocb->buf, iocb->length, iocb->offset);
	if (size != iocb->length)
		return SD_RES_EIO;
	return SD_RES_SUCCESS;
}

static int simple_store_read(uint64_t oid, struct siocb *iocb)
{
	int size = xpread(iocb->fd, iocb->buf, iocb->length, iocb->offset);
	if (size != iocb->length)
		return SD_RES_EIO;
	return SD_RES_SUCCESS;
}

static int simple_store_close(uint64_t oid, struct siocb *iocb)
{
	if (close(iocb->fd) < 0)
		return SD_RES_EIO;
	return SD_RES_SUCCESS;
}

static int simple_store_get_objlist(struct siocb *siocb)
{
	struct strbuf buf = STRBUF_INIT;
	int epoch = siocb->epoch;
	uint64_t *objlist = (uint64_t *)siocb->buf;
	DIR *dir;
	struct dirent *d;
	int ret = SD_RES_SUCCESS;

	strbuf_addf(&buf, "%s%08u/", obj_path, epoch);

	dprintf("%s\n", buf.buf);

	dir = opendir(buf.buf);
	if (!dir) {
		ret = SD_RES_EIO;
		goto out;
	}

	while ((d = readdir(dir))) {
		uint64_t oid;
		if (!strcmp(d->d_name, ".") || !strcmp(d->d_name, ".."))
			continue;

		oid = strtoull(d->d_name, NULL, 16);
		if (oid == 0)
			continue;

		objlist[siocb->length++] = oid;
	}
	closedir(dir);
out:
	strbuf_release(&buf);
	return ret;
}

static int simple_store_link(uint64_t oid, struct siocb *iocb, int tgt_epoch)
{
       char old[PATH_MAX], new[PATH_MAX];

       snprintf(old, sizeof(old), "%s%08u/%016" PRIx64, obj_path,
                tgt_epoch, oid);
       snprintf(new, sizeof(new), "%s%08u/%016" PRIx64, obj_path,
                iocb->epoch, oid);
       dprintf("link from %s to %s\n", old, new);
       if (link(old, new) == 0)
               return SD_RES_SUCCESS;

       if (errno == ENOENT)
               return SD_RES_NO_OBJ;

       return SD_RES_EIO;
}

static int simple_store_atomic_put(uint64_t oid, struct siocb *iocb)
{
	char path[PATH_MAX], tmp_path[PATH_MAX];
	int flags = O_DSYNC | O_RDWR | O_CREAT;
	int ret = SD_RES_EIO, epoch = iocb->epoch, fd;
	uint32_t len = iocb->length;

	snprintf(path, sizeof(path), "%s%08u/%016" PRIx64, obj_path,
		 epoch, oid);
	snprintf(tmp_path, sizeof(tmp_path), "%s%08u/%016" PRIx64 ".tmp",
		 obj_path, epoch, oid);

	fd = open(tmp_path, flags, def_fmode);
	if (fd < 0) {
		eprintf("failed to open %s: %m\n", tmp_path);
		goto out;
	}

	ret = write(fd, iocb->buf, len);
	if (ret != len) {
		eprintf("failed to write object. %m\n");
		ret = SD_RES_EIO;
		goto out_close;
	}


	ret = rename(tmp_path, path);
	if (ret < 0) {
		eprintf("failed to rename %s to %s: %m\n", tmp_path, path);
		ret = SD_RES_EIO;
		goto out_close;
	}
	dprintf("%"PRIx64"\n", oid);
	ret = SD_RES_SUCCESS;
out_close:
	close(fd);
out:
	return ret;
}

static int simple_store_format(struct siocb *iocb)
{
	char path[PATH_MAX];
	unsigned epoch = iocb->epoch, ret, i;
	const uint8_t name[] = "simple";

	dprintf("epoch %u\n", epoch);
	for (i = 1; i <= epoch; i++) {
		snprintf(path, sizeof(path), "%s%08u", obj_path, i);
		ret = rmdir_r(path);
		if (ret && ret != -ENOENT) {
			eprintf("failed to remove %s: %s\n", path, strerror(-ret));
			return SD_RES_EIO;
		}
	}

	if (set_cluster_store(name) < 0)
		return SD_RES_EIO;

	return SD_RES_SUCCESS;
}

struct store_driver simple_store = {
	.name = "simple",
	.init = simple_store_init,
	.open = simple_store_open,
	.write = simple_store_write,
	.read = simple_store_read,
	.close = simple_store_close,
	.get_objlist = simple_store_get_objlist,
	.link = simple_store_link,
	.atomic_put = simple_store_atomic_put,
	.format = simple_store_format,
};

add_store_driver(simple_store);
