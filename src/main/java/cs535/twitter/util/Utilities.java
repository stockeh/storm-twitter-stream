package cs535.twitter.util;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Utilities {

	/**
	 * Sort a map in descending order
	 * 
	 * @param <K>
	 * @param <V>
	 * @param map
	 * @param size
	 * @return
	 */
	public static <K, V extends Comparable<V>> Map<K, V> sortMapDecending(
			Map<K, V> map, int size) {
		return map.entrySet().stream()
				.sorted( Map.Entry
						.comparingByValue( Comparator.reverseOrder() ) )
				.limit( size )
				.collect( Collectors.toMap( Map.Entry::getKey,
						Map.Entry::getValue, (e1, e2) -> e1,
						LinkedHashMap::new ) );
	}

}
