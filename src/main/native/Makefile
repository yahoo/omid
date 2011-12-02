
########################################################################
#
# Copyright (c) 2011 Yahoo! Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License. See accompanying LICENSE file.
#
########################################################################

OSTYPE := $(shell uname -s)
CC = g++
LDSHARED = $(CC) -fPIC
TSO_COMMITHASHMAP_SO = libtso-commithashmap.so
ifeq ($(OSTYPE),Darwin)
TSO_COMMITHASHMAP_SO = libtso-commithashmap.jnilib
endif

INCLUDES=
ifeq ($(OSTYPE),Darwin) 
INCLUDES=-I/System/Library/Frameworks/JavaVM.framework/Headers 
else
INCLUDES=-I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux
endif

ifeq ($(OSTYPE),Darwin)
	LDFLAGS=-dynamiclib
else
	LDFLAGS=-shared -Wl,-soname=$(TSO_COMMITHASHMAP_SO)
endif


all: com_yahoo_omid_tso_CommitHashMap.h $(TSO_COMMITHASHMAP_SO)

nativelib.o: nativelib.cc
	$(CC) -fPIC -O3 $(INCLUDES) -c -o nativelib.o nativelib.cc 

$(TSO_COMMITHASHMAP_SO): nativelib.o $(DEP_LIBS)
	$(LDSHARED) -o $@ $(LDFLAGS) nativelib.o $(LIBS)

com_yahoo_omid_tso_CommitHashMap.h: nativelib.cc
	javah -classpath ../../../target/classes -jni com.yahoo.omid.tso.CommitHashMap

clean: 
	rm -f nativelib.o $(TSO_COMMITHASHMAP_SO)


