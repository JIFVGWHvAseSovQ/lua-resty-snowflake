PREFIX = /usr/local/lib
LUAJIT_LIB ?= /usr/local/lib
LUAJIT_INC ?= /usr/local/include/luajit-2.1

CC = gcc
CFLAGS = -Wall -Wextra -std=c11 -fPIC -g -O2  -I$(LUAJIT_INC)
LDFLAGS = -L$(LUAJIT_LIB) -shared -lluajit-5.1

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
