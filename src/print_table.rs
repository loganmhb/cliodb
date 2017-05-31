

use itertools::*;

use std::cmp::max;
use std::fmt::{self, Display, Formatter};

pub fn debug_table<A, B, C, D, E, F, G>(name: A,
                                        column_names: B,
                                        column_alignments: D,
                                        rows: E)
                                        -> impl Display
    where A: Into<String>,
          B: IntoIterator<Item = C>,
          C: Into<String>,
          D: IntoIterator<Item = Alignment>,
          E: IntoIterator<Item = F>,
          F: IntoIterator<Item = G>,
          G: Into<String>
{


    let name = name.into();
    let col_names = column_names.into_iter().map(|name| name.into()).collect_vec();
    let mut col_widths = col_names.iter().map(|name| name.len()).collect_vec();
    let rows = rows.into_iter().map(|r| r.into_iter().map(Into::into).collect_vec()).collect_vec();
    let col_align = column_alignments.into_iter().take(col_names.len()).collect_vec();

    for row in rows.iter() {
        assert_eq!(col_widths.len(), row.len());

        for (row, col_width) in zip(row, col_widths.iter_mut()) {
            *col_width = max(*col_width, row.len());
        }
    }

    let header = format!("| {} |",
                         zip(col_names, &col_widths)
                             .map(|(name, width)| format!("{:^1$}", name, width))
                             .join(" | "));

    let sep = header.chars()
        .map(|c| match c {
            '|' => "+",
            _ => "-",
        })
        .join("");

    TablePrinter {
        name: name,
        header: header,
        sep: sep,
        col_widths: col_widths,
        col_align: col_align,
        rows: rows,
    }
}

#[derive(Clone, Copy)]
pub enum Alignment {
    Left,
    Right,
    Center,
}

struct TablePrinter {
    name: String,
    header: String,
    sep: String,
    col_widths: Vec<usize>,
    col_align: Vec<Alignment>,
    rows: Vec<Vec<String>>,
}

impl TablePrinter {
    fn fmt_row(&self, row: &[String]) -> String {
        format!("| {} |",
                izip!(row, &self.col_align, &self.col_widths)
                    .map(|(r, a, w)| match *a {
                        Alignment::Left => format!("{:<1$}", r, w),
                        Alignment::Center => format!("{:^1$}", r, w),
                        Alignment::Right => format!("{:>1$}", r, w),
                    })
                    .join(" | "))
    }
}

impl Display for TablePrinter {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        writeln!(f, "{}:", self.name)?;
        writeln!(f, "{}", self.sep)?;
        writeln!(f, "{}", self.header)?;
        writeln!(f, "{}", self.sep)?;

        for row in self.rows.iter() {
            writeln!(f, "{}", self.fmt_row(&*row))?;
        }

        writeln!(f, "{}", self.sep)
    }
}
