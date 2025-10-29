# # TBB_PREFIX := /opt/tbb-static
# # TBB_INC    := $(TBB_PREFIX)/include
# # TBB_LIB    := $(TBB_PREFIX)/lib

# # CC=g++
# # CFLAGS=-std=c++11 -g -Wall -pthread -I./ -I$(TBB_INC)
# CFLAGS=-std=c++11 -g -Wall -pthread -I./
# CC=/home/timatm/x86_64-linux-musl-cross/bin/x86_64-linux-musl-g++
# # CFLAGS=-g -Wall -pthread -I./
# LDFLAGS = -static -static-libstdc++ -static-libgcc -pthread

# DISABLED_SRCS := db/redis_db.cc
# # LDFLAGS=-lpthread -lhiredis -libtbb
# # SUBDIRS=core db redis
# SUBSRCS_ALL := $(wildcard core/*.cc) $(wildcard db/*.cc)
# SUBSRCS := $(filter-out $(DISABLED_SRCS),$(SUBSRCS_ALL))
# OBJECTS := $(SUBSRCS:.cc=.o)
# EXEC=ycsbc

# all: $(SUBDIRS) $(EXEC)

# $(SUBDIRS):
# 	$(MAKE) -C $@

# $(EXEC): $(wildcard *.cc) $(OBJECTS)
# 	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@

# clean:
# 	for dir in $(SUBDIRS); do \
# 		$(MAKE) -C $$dir $@; \
# 	done
# 	$(RM) $(EXEC)

# .PHONY: $(SUBDIRS) $(EXEC)



# 选择 musl 编译器（隐式规则用 CXX！）
CXX := /home/timatm/x86_64-linux-musl-cross/bin/x86_64-linux-musl-g++

# 头文件搜索路径 & 功能开关
CPPFLAGS := -I. -Idb -Icore -I/home/timatm/simplessd_fully_system/include -DENABLE_REDIS=0 -DUSE_TBB=0

# 编译/链接选项
CXXFLAGS := -std=c++11 -g -Wall -pthread  -mfpmath=sse -msse2 

LDFLAGS  := -static -static-libstdc++ -static-libgcc -pthread -no-pie 
# LDLIBS   := /home/timatm/simplessd_fully_system/util/m5/build/x86/out/libm5.a

# 禁用的源文件（禁用 Redis 后端）
DISABLED_SRCS := db/redis_db.cc

# 采集源码
SUBSRCS_ALL := $(wildcard core/*.cc) $(wildcard db/*.cc)
SUBSRCS     := $(filter-out $(DISABLED_SRCS),$(SUBSRCS_ALL))
OBJECTS     := $(SUBSRCS:.cc=.o)

TOP_SRCS    := $(wildcard *.cc)      # e.g., ycsbc.cc（含 main）
TOP_OBJS    := $(TOP_SRCS:.cc=.o)    # ← 必须：生成 ycsbc.o

EXEC        := ycsbc

# 只在顶层编译，不再递归进子目录
all: $(EXEC)

%.o: %.cc
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

# 链接：先放对象文件，再放库；库放最后（已用 $(LDLIBS)）
$(EXEC): $(OBJECTS) $(TOP_OBJS)
	$(CXX) $(LDFLAGS) $^ $(LDLIBS) -o $@

clean:
	$(RM) $(OBJECTS) $(TOP_OBJS) $(EXEC)

.PHONY: all clean
