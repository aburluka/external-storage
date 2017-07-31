SOURCES := $(shell find . 2>&1 | grep -E '.*\.(c|h|go)$$')

.DEFAULT: ploop

ploop: $(SOURCES)
	go build -o ploop .

install: ploop
	mkdir -p $(DESTDIR)/usr/libexec/kubernetes/kubelet-plugins/volume/exec/virtuozzo~ploop/
	cp ploop $(DESTDIR)/usr/libexec/kubernetes/kubelet-plugins/volume/exec/virtuozzo~ploop/

wrapper-journald: ploop
	cp ploop-journld.sh /usr/libexec/kubernetes/kubelet-plugins/volume/exec/virtuozzo~ploop/ploop
	cp ploop /usr/libexec/kubernetes/kubelet-plugins/volume/exec/virtuozzo~ploop/ploop.bin

wrapper-file: ploop
	cp ploop-file.sh /usr/libexec/kubernetes/kubelet-plugins/volume/exec/virtuozzo~ploop/ploop
	cp ploop /usr/libexec/kubernetes/kubelet-plugins/volume/exec/virtuozzo~ploop/ploop.bin

clean:
	rm -f ploop
