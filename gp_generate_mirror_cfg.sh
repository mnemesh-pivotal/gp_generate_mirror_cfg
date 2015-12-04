#!/bin/bash
#  Filename:      gp_generate_mirror_cfg.sh
#  Status:        Directed Access
#  Author:        M Nemesh
#  Contact:       mnemesh@pivotal.io
#  Release date:  December 2015
#  Release stat:  Pivotal Inc
#                 Copyright (c) Pivotal Inc 2015. All Rights Reserved
#  Brief desc:    This script creates input map file for gpmovemirrors, in order to migrate a system from group or spread mirroring to block mirroring.
#
#                 The only required input for the script is a file containing a list of server names, one per line, in order by group.
#                 The server names are the hosts already defined in the existing Greenplum instance.
#                 Mirrored host blocks are created based on the order of the input file and the specified block group size.
#                 When creating the input file, you should consider creating host blocks based on rack location.
#                 Blocks within a rack reduce east/west network traffic which typically has lower available bandwidth then within a rack.
#                 The number of hosts must be a multiple of the block group size, otherwise the program will exit with an error message.
#

#
# Check user running the script is gpadmin
#
if [ "$(id -un)" != "gpadmin" ]; then
	printf "\nERROR: This script must be run as gpadmin.  Exiting...\n\n" >&2
	exit 1
fi

#
# Trap user generated signals to cleanup before exiting
#
trap "SIGSET=TRUE;cleanup;exit" 2 3 15

#
# Set environment default values
#
database=postgres
mirror_map_dir=/tmp/mirror_map.$$
mirror_map_file=${mirror_map_dir}/mirrormap
tmpfile1=${mirror_map_dir}/tmpfile1
tmpfile2=${mirror_map_dir}/tmpfile2
mirror_group_file=""
mirror_cfg_file=~gpadmin/movemirrors.cfg
block_group_size=4
declare -a host_name_array

#
# cleanup function invoked from end of execution or trapped signal
#
cleanup()
{
	trap ":" 2 3
	if [ "$SIGSET" ]
	then
		printf "\nERROR: User canceled '$(basename $0)'.  Exiting...\n\n" >&2
	fi
	#
	# Cleanup tmp files
	#
	[ ! -z "${mirror_map_dir}" ] && [ -d ${mirror_map_dir} ] && rm -rf ${mirror_map_dir}

	#
	# Drop working schema
	#
	PGOPTIONS='--client-min-messages=warning' psql -AXqt -v datfile="'${mirror_map_file}'" -d ${database} <<-EOS
	drop schema if exists _xXx_movemirrors_xXx_ cascade;
	EOS
}

#
# Usage statement function
#
usage()
{
	printf "\nUSAGE: $(basename $0) -i infile [ -b size -d database -o outfile ]\n" >&2
	printf "  -i infile\t\tFILE containing host names, listed in grouping order\n" >&2
	printf "  [ -b SIZE ]\t\toptional number of hosts within a block group, default is 4\n" >&2
	printf "  [ -d DATABASE ]\toptional database name to create working schema in, default is ${database}\n" >&2
	printf "  [ -o OUTFILE ]\toptional output file name, dafault is ~gpadmin/movemirrors.cfg\n" >&2
	printf "\n" >&2
}

#
# Get command line parameters
#
while getopts ":i:o:d:b:" opt; do
	case ${opt} in
		i)
			mirror_group_file=${OPTARG}
			;;
		o)
			mirror_cfg_file=${OPTARG}
			;;
		d)
			database=${OPTARG}
			;;
		b)
			block_group_size=${OPTARG}
			;;
		h|?|*)
			usage
			exit 0
			;;
	esac
done

#
# Validate command line parameters
#
if [ -z "${mirror_group_file}" ]; then
	printf "\nERROR: No input file specified.\n" >&2
	usage
	exit 1
elif [ ! -f ${mirror_group_file} ]; then
	printf "\nERROR: Input file '%s' does not exist.  Exiting...\n\n" ${mirror_group_file} >&2
	exit 1
fi

if [ ! -z "${database}" ]; then
	psql -AXqt -d ${database} <<-EOS 2> /dev/null
	\q
	EOS
	if [ ${?} -ne 0 ]; then
		printf "\nERROR: Database '%s' does not exist.  Exiting...\n\n" >&2
		exit 1
	fi
fi

if [ ${block_group_size} -lt 2 ]; then
	printf "\nERROR: Block group size must be greater than 1.  Exiting...\n\n" >&2
	
	exit 1
fi

#
# Create working directory
#
mkdir ${mirror_map_dir}
if [ ${?} -ne 0 ]; then
	printf "\nERROR: Unable to create working directory '%s', please check permissions and try again.  Exiting...\n\n" ${mirror_map_dir} >&2
	exit 1
fi

#
# Check if the output file can be generated
#
touch ${mirror_cfg_file}
if [ $? -ne 0 ]; then
	printf "\nERROR: Cannot create output file '%s', please check directory permissions and try again.  Exiting...\n\n" ${mirror_cfg_file} >&2
	cleanup
	exit 1
fi

#
# Perform sanity checks before executing
#
mirror_host_count=$(grep -c . ${mirror_group_file})

primary_host_count=$(psql -AXqt -d ${database} <<-EOS
select count(1) from (
	select distinct g.address
	from
		gp_segment_configuration g
	where
		g.preferred_role='p' and
		content != -1
) as x;
EOS
)

primary_instrance_count=$(psql -AXqt -d ${database} <<-EOS
select count(1) from gp_segment_configuration
where
	address in (
		select g.address
		from
			gp_segment_configuration g
		where
			g.preferred_role='p' and
			content != -1
		group by 1
		order by 1
		limit 1
	)
	and preferred_role='p' and
	content != -1;
EOS
)

segments=$(psql -AXqt -d ${database} <<-EOS
select count(1) from gp_segment_configuration
where
        address in (
                select g.address
                from
                        gp_segment_configuration g
                where
                        g.preferred_role='p' and
                        content != -1
                group by 1
                order by 1
                limit 1
        )
        and preferred_role='p' and
        content != -1;
EOS
)

if [ ${primary_host_count} -ne ${mirror_host_count} ]; then
	printf "\nERROR: The number of primary hosts (%d) in the database does not match the number of block mirror hosts (%d) in the input file.  Exiting...\n\n" ${primary_host_count} ${mirror_host_count} >&2
	cleanup
	exit 1
fi

if [ $((${primary_host_count}%${block_group_size})) -ne 0 ]; then
	printf "\nERROR: The number of primary hosts, %s, must be a multiple of the block group size, %s.  Exiting...\n\n" ${primary_host_count} ${block_group_size} >&2
	cleanup
	exit 1
fi

psql -AXqt -d ${database} <<-EOS > ${tmpfile1}
select distinct g.address
from
	gp_segment_configuration g
where
	g.preferred_role='p' and
	content != -1
order by
	g.address;
EOS

sort ${mirror_group_file} > ${tmpfile2}

paste ${tmpfile1} ${tmpfile2} | awk -v nomatch=0 ' { if ($1 != $2) nomatch=1 } END { if (nomatch == 1) printf "Server hostnames in the database do not match the hostnames in the input file.  Check the output below.\n\n"; exit nomatch } '
awkrc=$?

if [ ${awkrc} -eq 1 ]; then
	paste ${tmpfile1} ${tmpfile2} | cat -v
	printf "\nExiting...\n\n" >&2
	cleanup
	exit 1
fi

printf "Generating gpmovemirrors configuration file, this may take a few moments, please be patient...\n"

#
# Create the host map for the mirrors
#
host_name_array=( $(grep . ${mirror_group_file}) )

let block_group_count=${#host_name_array[@]}/${block_group_size}

for g in $(seq ${block_group_count}); do
	let g=${g}-1
	firstloop=true
	for b in $(seq ${block_group_size}); do
		s=1
		if [ ${firstloop} ]; then h=${b}; firstloop=false; else h=1; fi
		while [ ${s} -le ${segments} ]; do
			while [ ${h} -le ${block_group_size} ]; do
				if [ ${b} -ne ${h} ]
				then
					let s=${s}+1
					let a="(${h} - 1) + (${block_group_size} * ${g})"
					printf "%s\n" ${host_name_array[${a}]}
				fi
				let h=${h}+1
				if [ ${s} -gt ${segments} ]; then break; fi
			done
			h=1
		done
	done
done > ${mirror_map_file}

#
# Clean up old schema if it exists and create new schema with work tables
#
PGOPTIONS='--client-min-messages=warning' psql -AXqt -v datfile="'${mirror_map_file}'" -d ${database} <<-EOS
drop schema if exists _xXx_movemirrors_xXx_ cascade;
create schema _xXx_movemirrors_xXx_;
set search_path to _xXx_movemirrors_xXx_,"\$user",public;
create table _xXx_hostmap_xXx_ (seqid serial, address text) distributed randomly;
copy _xXx_hostmap_xXx_ (address) from :datfile;
create table _xXx_primaries_xXx_ (seqid serial, content numeric, location text) distributed randomly;
create table _xXx_mirrors_xXx_ (content numeric, location text) distributed randomly;
create table _xXx_newmirrors_xXx_ (content numeric, location text) distributed randomly;
EOS

#
# Create serialized list of primaries based on mirror group file
#
for i in $(cat ${mirror_group_file}); do
psql -Xq -v maphost="'${i}'" -d ${database} <<-EOS
set search_path to _xXx_movemirrors_xXx_,"\$user",public;
-- Get primaries
insert into _xXx_primaries_xXx_ (content, location) (select
	g.content, g.address || ':' || g.port || ':' || p.fselocation
from
	gp_segment_configuration g
	, pg_filespace_entry p
	, pg_filespace f
where
	f.fsname='pg_system' and
	f.oid = p.fsefsoid and
	g.preferred_role='p' and
	g.dbid = p.fsedbid and
	g.address = :maphost and
	content != -1
order by
	p.fsefsoid
	, g.content
);
EOS
done

#
# Generate mirror mapping with output going to config file
#
psql -AXqt -d ${database} <<-EOS > ${mirror_cfg_file}
set search_path to _xXx_movemirrors_xXx_,"\$user",public;

-- Get current pg_system mirrors

insert into _xXx_mirrors_xXx_ (
	  content
	, location)
(
	select
		  x.content
		, x.address || ':' || x.port || ':' || array_to_string(array_agg(x.mirror),':')
	from
	(
		select
			g.content, g.address, g.port, p.fselocation as mirror
		from
			  gp_segment_configuration g
			, pg_filespace_entry p
			, pg_filespace f
			, _xXx_primaries_xXx_
		where
			f.fsname = 'pg_system' and
			f.oid = p.fsefsoid and
			preferred_role='m' and
			g.dbid = p.fsedbid and
			g.content != -1
		group by
			g.dbid, g.content, g.address, g.port, p.fselocation
		order by
			g.dbid, g.content, g.address, g.port, p.fselocation
	) as x
	group by
		 x.content, x.address, x.port
	order by
		 x.content
);

-- Generate new mirror ports and locations for all filespaces

insert into _xXx_newmirrors_xXx_
(
	  content
	, location
)
(
	select
		  x.content
		, ':' || x.port || ':' || replication_port || ':' || array_to_string(array_agg(x.location),':')
	from (
		select
			g.dbid, g.content, g.port, g.replication_port, p.fselocation as location
		from
			  gp_segment_configuration g
			, pg_filespace_entry p
			, pg_filespace f
			, _xXx_primaries_xXx_
		where
			f.oid = p.fsefsoid and
			preferred_role='m' and
			g.dbid = p.fsedbid and
			g.content != -1
		group by
			g.content, f.oid, g.dbid, g.port, g.replication_port, p.fselocation
		order by
			g.content, f.oid desc, g.dbid
	) as x
	group by
		x.dbid, x.content, x.port, x.replication_port
	order by
		x.dbid, x.content
);

-- Begin configuration file generation

-- Output filespace order

select 'filespaceOrder=' || filespaces
from
(
	select
		  x.fspace
		, array_to_string(array_agg(x.fsname), ':') as filespaces
	from
	(
		select
			  '1' as fspace
			, fsname
		from
			pg_filespace
		where
			fsname != 'pg_system'
		order by
			oid
	) as x
group by
	x.fspace
order by
	x.fspace
) as q;

-- Output new mirror mapping only for mirrors that need to be moved

select
	  m.location || ' ' || h.address || n.location as newmirror
from
	  _xXx_hostmap_xXx_ h
	, _xXx_primaries_xXx_ p
	, _xXx_mirrors_xXx_ m
	, _xXx_newmirrors_xXx_ n
where
	    p.seqid = h.seqid
	and p.content = m.content
	and p.content = n.content
	and split_part(m.location, ':', 1) != h.address
order by
	p.seqid;
EOS

#
# Report job complete and cleanup before exiting
#
printf "Configuration file '%s' has been created.\nPlease check the file before proceeding with gpmovemirrors.\n" ${mirror_cfg_file}
cleanup
exit 0
