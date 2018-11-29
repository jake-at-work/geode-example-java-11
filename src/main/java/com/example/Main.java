/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.example;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;

public class Main {

  public static void main (String[] args) throws Exception {
    // connect to the locator using default port 10334
    ClientCache cache = new ClientCacheFactory()
        .addPoolLocator("127.0.0.1", 10334)
        .setPoolSubscriptionEnabled(true)
        .set("log-level", "WARN")
        .setPdxSerializer(
            new ReflectionBasedAutoSerializer("com\\.example\\..*"))
        .create();

    // create a local region that matches the server region
    Region<Integer, Object> region =
        cache.<Integer, Object>createClientRegionFactory(ClientRegionShortcut.PROXY)
            .create("example-region");


    helloWorldGetPut(region);

    helloWorldQuery(region);

    helloWorldCQ(region);

    helloWorldAutoPdx(region);

    helloWorldAutoPdxQuery(region);

    cache.close();

  }

  private static void helloWorldGetPut(Region<Integer, Object> region) {
    region.put(1, "Hello world from get/put!");
    String v =  (String) region.get(1);
    System.out.println(v);
  }

  private static void helloWorldQuery(Region<Integer, Object> region) throws Exception {
    region.put(1, "Hello world from query!");
    SelectResults<String> results = region.query("SELECT * FROM /example-region");
    for (String v : results) {
      System.out.println(v);
    }
  }

  private static void helloWorldCQ(Region<Integer, Object> region) throws Exception {
    AtomicBoolean done = new AtomicBoolean(false);
    CqAttributesFactory cqf = new CqAttributesFactory();
    cqf.addCqListener(new CqListener() {
      @Override
      public void onEvent(CqEvent cqEvent) {
        String v = (String) cqEvent.getNewValue();
        System.out.println(v);
        done.set(true);
      }

      @Override
      public void onError(CqEvent cqEvent) {
        done.set(true);
      }
    });

    CqAttributes attributes = cqf.create();
    CqQuery cq = region.getRegionService().getQueryService().newCq("SELECT * from /example-region", attributes);
    cq.execute();

    region.put(1, "Hello world from cq!");

    while (!done.get()) ;
    cq.close();
  }

  private static void helloWorldAutoPdx(Region<Integer, Object> region) {
    region.put(1, new AutoSerializableObject("Hello World from auto PDX!"));
    AutoSerializableObject v = (AutoSerializableObject) region.get(1);
    System.out.println(v.getValue());
  }


  private static void helloWorldAutoPdxQuery(Region<Integer, Object> region) throws Exception {
    region.put(1, new AutoSerializableObject("Hello World from auto PDX Query!"));
    SelectResults<String> results = region.query("SELECT e.toString() FROM /example-region e");
    for (String v : results) {
      System.out.println(v);
    }
  }

}
