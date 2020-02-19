package cs535.twitter.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class Utilities {

	/**
	 * Sort the map depending on the specified song data type in ascending
	 * or descending order.
	 * 
	 * @param map HashMap of the values
	 * @param type type of data
	 * @param descending true for descending, false for ascending
	 * @return a new map of sorted <K, V> pairs.=
	 */
	public static Map<String, Integer> sortMapByValue(Map<String, Integer> map,
			boolean descending) {

		Comparator<Entry<String, Integer>> comparator =
				Map.Entry.<String, Integer>comparingByValue();

		if ( descending )
		{
			comparator = Collections.reverseOrder( comparator );
		}
		return map.entrySet().stream().sorted( comparator )
				.collect( Collectors.toMap( Map.Entry::getKey,
						Map.Entry::getValue, (e1, e2) -> e1,
						LinkedHashMap::new ) );
	}

}
