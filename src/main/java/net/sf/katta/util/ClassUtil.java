/**
 * Copyright 2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.sf.katta.util;

import java.lang.reflect.Field;

import org.I0Itec.zkclient.ExceptionUtil;

public class ClassUtil {

  /**
   * 
   * @param <T>
   * @param className
   * @param instanceOfClass
   * @return the class object for a given class name
   */
  @SuppressWarnings("unchecked")
  public static <T> Class<T> forName(String className, Class<T> instanceOfClass) {
    try {
      Class<?> loadedClass = Class.forName(className);
      if (!instanceOfClass.isAssignableFrom(loadedClass)) {
        throw new IllegalStateException("Class " + className + " does not implement " + instanceOfClass.getName());
      }
      return (Class<T>) loadedClass;
    } catch (ClassNotFoundException e) {
      throw ExceptionUtil.convertToRuntimeException(e);
    }
  }

  /**
   * @param <T>
   * @param clazz
   * @return a new instance of the given class
   */
  public static <T> T newInstance(Class<T> clazz) {
    try {
      return clazz.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("could not instantiate class " + clazz.getName(), e);
    }
  }

  /**
   * 
   * @param object
   * @param fieldName
   * @return the value of the (private) field of the given object with the given
   *         name
   */
  public static Object getPrivateFieldValue(Object object, String fieldName) {
    return getPrivateFieldValue(object.getClass(), object, fieldName);
  }

  /**
   * @param clazz
   * @param object
   * @param fieldName
   * @return the value of the (private) field of the given object declared in
   *         the given class with the given name
   */
  public static Object getPrivateFieldValue(Class<?> clazz, Object object, String fieldName) {
    Field field = null;
    do {
      try {
        field = clazz.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
        // proceed with superclass
      }
      clazz = clazz.getSuperclass();
    } while (clazz != null);
    try {
      if (field == null) {
        throw new NoSuchFieldException("no field '" + fieldName + "' in object " + object);
      }
      field.setAccessible(true);
      return field.get(object);
    } catch (Exception e) {
      throw ExceptionUtil.convertToRuntimeException(e);
    }
  }

}
