PREFIX = /usr/local/lib
LUAJIT_LIB ?= /usr/local/lib
LUAJIT_INC ?= /usr/local/include/luajit-2.1

CC = gcc
CFLAGS = -O2 -fPIC -Wall -Wextra -std=c11 -D_GNU_SOURCE -D_POSIX_C_SOURCE=200809L -I$(LUAJIT_INC)
LDFLAGS = -L$(LUAJIT_LIB) -lrt -shared -lluajit-5.1

TARGET = snowflake.so

.PHONY: all clean install uninstall

all: $(TARGET)

$(TARGET): snowflake.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

install:
	install -d $(DESTDIR)$(PREFIX)
	install -m 644 $(TARGET) $(DESTDIR)$(PREFIX)
	ldconfig || true

uninstall:
	rm -f $(DESTDIR)$(PREFIX)/$(TARGET)
	ldconfig || true

clean:
	rm -f $(TARGET)
