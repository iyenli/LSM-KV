QT -= gui

CONFIG += c++14 console
CONFIG -= app_bundle

# You can make your code fail to compile if it uses deprecated APIs.
# In order to do so, uncomment the following line.
#DEFINES += QT_DISABLE_DEPRECATED_BEFORE=0x060000    # disables all the APIs deprecated before Qt 6.0.0

SOURCES += \
    bloom.cc \
#    compaction-performance.cc \
    compare.cc \
#    correctness.cc \
    kvstore.cc \
#    persistence.cc \
    skiptable.cc \
    sstable.cc \
    test-performance.cc \
    tool.cc


# Default rules for deployment.
qnx: target.path = /tmp/$${TARGET}/bin
else: unix:!android: target.path = /opt/$${TARGET}/bin
!isEmpty(target.path): INSTALLS += target

HEADERS += \
    MurmurHash3.h \
    bloom.h \
    constant.h \
    kvstore.h \
    kvstore_api.h \
    skiptable.h \
    sstbale.h \
    test.h \
    tool.h \
    utils.h
