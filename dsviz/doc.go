/*Package dsviz renders the viz component of a dataset, returning a qfs.File of
data

HTML rendering uses go's html/template package to generate html documents from
an input dataset. It's API has been adjusted to use lowerCamelCase instead of
UpperCamelCase naming conventions

	outline: html viz templates
		HTML template should expose a number of helper template functions, along
		with a  dataset document at ds. Exposing the dataset  document as "ds"
		matches our conventions for referring to a dataset elsewhere, and allows
		access to all defined parts of a dataset.
		HTML visualization is built atop the
		[go template syntax](https://golang.org/pkg/text/template/#hdr-Functions)
		types:
			{{ ds }}
				the dataset being visualized, ds can have a number of components like
				commit, meta, transform, body, all of which have helpful fields for
				visualization. Details of the dataset document are outlined in the
				dataset document definition
		functions:
			{{ allBodyEntries }}
				load the full dataset body
			{{ bodyEntries offset limit }}
				get body entries within an offset/limit range. passing offset: 0,
				limit: -1 returns the entire body
			{{ filesize }}
				convert byte count to kb/mb/etc string
			{{ title }}
				give the title of a dataset
*/
package dsviz
