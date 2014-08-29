package de.tuberlin.cit.test.twittersentiment.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

public class Utils {
	public static <K, V> Map<K, V> sortMapByEntry(Map<K, V> unsortedMap, Comparator<Map.Entry<K, V>> comparator) {
		LinkedList<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(unsortedMap.entrySet());

		Collections.sort(list, comparator);

		Map<K, V> sortedMap = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}

	public static <K, V> Map<K, V> slice(Map<K, V> map, int n) {
		LinkedHashMap<K, V> slicedMap = new LinkedHashMap<K, V>();
		Iterator<Map.Entry<K, V>> iterator = map.entrySet().iterator();
		for (int i = 0; i < n && iterator.hasNext(); i++) {
			Map.Entry<K, V> entry = iterator.next();
			slicedMap.put(entry.getKey(), entry.getValue());
		}
		return slicedMap;
	}
}
