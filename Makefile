CC = gcc
CFLAGS = -g -fPIC -Wall
LDFLAGS = -shared

PREFIX = /usr/local/lfs
TARGET = libsnowflake.so

.PHONY: all clean install uninstall

all: $(TARGET)

$(TARGET): snowflake.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $

install:
	install -d $(DESTDIR)$(PREFIX)
	install -m 644 $(TARGET) $(DESTDIR)$(PREFIX)
	ldconfig || true

uninstall:
	rm -f $(DESTDIR)$(PREFIX)/$(TARGET)
	ldconfig || true

clean:
	rm -f $(TARGET)
