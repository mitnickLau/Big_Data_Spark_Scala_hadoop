package com.shanghaiuniv.ct.common.bean;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * 数据的来源
 */
public interface DataIn extends Closeable {
    public void setPath(String path);

    public Object read() throws IOException;

    //利用了反射的技术原理
    public <T extends Data> List<T> read(Class<T> clazz) throws IOException;
}
