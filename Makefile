CC = gcc
CFLAGS = -g -fPIC -Wall
LDFLAGS = -shared

PREFIX = /usr/local/lib
TARGET = libsnowflake.so

.PHONY: all clean install uninstall

all: $(TARGET)

$(TARGET): snowflake.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $

install:
	install -d $(DESTDIR)$(INSTALL_DIR)
	install -m 644 $(TARGET) $(DESTDIR)$(INSTALL_DIR)
	ldconfig || true

uninstall:
	rm -f $(DESTDIR)$(INSTALL_DIR)/$(TARGET)
	ldconfig || true

clean:
	rm -f $(TARGET)
