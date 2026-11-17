//! The picture of the GRAPH screen: a run's tasks in columns by level,
//! roots on the left, with a line and an arrow for every dependency.
//!
//! The picture is made of character cells. A run's tasks and their needs
//! never change, so it is computed once per run; drawing then only looks at
//! the part on screen. It is laid out in the manner of Sugiyama:
//!
//! 1. A dependency that skips levels crosses the levels in between on a row
//!    of its own, a waypoint, so that no line runs through a name. The long
//!    dependencies of one task share their waypoints.
//! 2. A few sweeps reorder every column by the average position of the
//!    neighbours in the column next to it, which removes most crossings.
//! 3. Every task takes the median row of the tasks it needs, as far as the
//!    order allows, so that chains stay straight; then the roots move to
//!    the middle of what they lead to.
//! 4. Between two columns, the lines of a task needed by several others
//!    share one vertical lane (a trunk), and so do the lines of tasks that
//!    each lead only to one same task (a collector). Lanes that would
//!    overlap get separate columns of the gap.

use std::collections::BTreeMap;

use super::backend::TaskRow;
use super::theme::fit;

/// Longest name shown in full.
const NAME_WIDTH: usize = 24;
/// Passes of the reordering step, each one down the columns and back.
const SWEEPS: usize = 4;

// The directions in which a line leaves a cell.
const UP: u8 = 1;
const DOWN: u8 = 2;
const LEFT: u8 = 4;
const RIGHT: u8 = 8;

pub(crate) struct Picture {
    /// Every dependency, as (the task needed, the task that needs it).
    pub(crate) edges: Vec<(usize, usize)>,
    /// Per task: the row and the first cell of its label.
    pub(crate) at: Vec<(usize, usize)>,
    /// Per task: its name as shown, cut if too long.
    pub(crate) names: Vec<String>,
    /// Per task: its column, which is its level.
    pub(crate) column_of: Vec<usize>,
    /// Per column: its tasks, top to bottom.
    pub(crate) columns: Vec<Vec<usize>>,
    /// Per column: the cell where it starts.
    pub(crate) column_x: Vec<usize>,
    /// Per task: the tasks it needs.
    pub(crate) needs: Vec<Vec<usize>>,
    /// Per task: the tasks that need it.
    pub(crate) dependents: Vec<Vec<usize>>,
    /// The arrow heads in front of the tasks.
    pub(crate) arrows: Vec<Arrow>,
    strokes: Vec<Stroke>,
    pub(crate) width: usize,
    pub(crate) height: usize,
}

/// An arrow head, with the dependencies that end there.
pub(crate) struct Arrow {
    pub(crate) row: usize,
    pub(crate) x: usize,
    pub(crate) edges: Vec<u32>,
}

/// A part of the picture: `width` cells from `x`, `height` rows from `y`.
pub(crate) struct Window {
    pub(crate) x: usize,
    pub(crate) y: usize,
    pub(crate) width: usize,
    pub(crate) height: usize,
}

/// A straight piece of line, with the dependencies it belongs to (their
/// positions in [`Picture::edges`]), which decide its colour.
struct Stroke {
    shape: Shape,
    edges: Vec<u32>,
}

#[derive(Clone, Copy)]
enum Shape {
    Across {
        row: usize,
        left: usize,
        right: usize,
    },
    Down {
        x: usize,
        top: usize,
        bottom: usize,
    },
}

/// A line between two neighbouring columns of the layout, from a task or
/// a waypoint to another.
struct Link {
    from: usize,
    to: usize,
    edges: Vec<u32>,
}

/// How the links of one gap between columns are drawn.
struct Gap {
    bundles: Vec<Bundle>,
    lanes: usize,
}

/// Links drawn together.
struct Bundle {
    kind: Kind,
    links: Vec<usize>,
    lane: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// The links of one task leading to several.
    Trunk,
    /// The links of several tasks leading to one, and nowhere else.
    Collector,
    /// A link alone that has to change row.
    Single,
    /// A link alone that stays on its row: no lane.
    Straight,
}

impl Picture {
    /// Lays out the tasks of a run. `levels` must hold every task once, and
    /// a task's needs must all be in earlier levels.
    pub(crate) fn new(tasks: &[TaskRow], levels: &[Vec<usize>]) -> Self {
        let count = tasks.len();
        let mut column_of = vec![0; count];
        for (level, members) in levels.iter().enumerate() {
            for &task in members {
                column_of[task] = level;
            }
        }
        // Every dependency once, ordered by task and then by need. A need
        // that is not in an earlier level would mean a broken graph: such a
        // dependency is left out rather than drawn backwards.
        let mut edges = Vec::new();
        let mut needs = vec![Vec::new(); count];
        let mut dependents = vec![Vec::new(); count];
        for (task, row) in tasks.iter().enumerate() {
            let mut wanted = row.needs.clone();
            wanted.sort_unstable();
            wanted.dedup();
            for need in wanted {
                if need < count && column_of[need] < column_of[task] {
                    edges.push((need, task));
                    needs[task].push(need);
                    dependents[need].push(task);
                }
            }
        }

        // The nodes of the layout are the tasks, then the waypoints.
        let mut node_column = column_of.clone();
        let mut links = Vec::new();
        let mut leaving: Vec<Vec<u32>> = vec![Vec::new(); count];
        for (id, &(need, _)) in edges.iter().enumerate() {
            leaving[need].push(id as u32);
        }
        for (source, out) in leaving.iter().enumerate() {
            let level = |edge: u32| column_of[edges[edge as usize].1];
            let Some(farthest) = out.iter().map(|&edge| level(edge)).max() else {
                continue;
            };
            // From the task, column by column: the dependents found in each
            // one, and a waypoint for the dependencies going further.
            let mut previous = source;
            for column in column_of[source] + 1..=farthest {
                for &edge in out.iter().filter(|&&edge| level(edge) == column) {
                    links.push(Link {
                        from: previous,
                        to: edges[edge as usize].1,
                        edges: vec![edge],
                    });
                }
                if column < farthest {
                    let waypoint = node_column.len();
                    node_column.push(column);
                    let passing = out.iter().copied().filter(|&edge| level(edge) > column);
                    links.push(Link {
                        from: previous,
                        to: waypoint,
                        edges: passing.collect(),
                    });
                    previous = waypoint;
                }
            }
        }
        let nodes = node_column.len();
        let mut before = vec![Vec::new(); nodes];
        let mut after = vec![Vec::new(); nodes];
        for link in &links {
            after[link.from].push(link.to);
            before[link.to].push(link.from);
        }

        // The order within the columns: tasks by name to begin with, then
        // the waypoints, then sweeps.
        let mut order = levels.to_vec();
        for node in count..nodes {
            order[node_column[node]].push(node);
        }
        let mut position = vec![0; nodes];
        for column in &order {
            for (index, &node) in column.iter().enumerate() {
                position[node] = index;
            }
        }
        for sweep in 0..=SWEEPS {
            for column in order.iter_mut().skip(1) {
                reorder(column, &before, &mut position);
            }
            if sweep == SWEEPS {
                break;
            }
            for column in order.iter_mut().rev().skip(1) {
                reorder(column, &after, &mut position);
            }
        }

        // Rows, column by column, then the roots again.
        let mut row = vec![0; nodes];
        for (index, column) in order.iter().enumerate() {
            let mut free = 0;
            for &node in column {
                let wanted = match index {
                    0 => free,
                    _ => median(before[node].iter().map(|&n| row[n])).unwrap_or(free),
                };
                row[node] = wanted.max(free);
                free = row[node] + 1;
            }
        }
        if let Some(roots) = order.first() {
            let mut free = 0;
            for &node in roots {
                let wanted = median(after[node].iter().map(|&n| row[n])).unwrap_or(free);
                row[node] = wanted.max(free);
                free = row[node] + 1;
            }
        }
        let height = row.iter().max().map_or(0, |&last| last + 1);

        // Widths: the longest label of each column and a space; between two
        // columns, the lanes of the gap and room for an arrow.
        let names: Vec<String> = tasks
            .iter()
            .map(|task| match task.name.chars().count() {
                length if length > NAME_WIDTH => fit(&task.name, NAME_WIDTH),
                _ => task.name.clone(),
            })
            .collect();
        let label = |task: usize| names[task].chars().count() + 2;
        let column_width: Vec<usize> = levels
            .iter()
            .map(|level| level.iter().map(|&task| label(task)).max().unwrap_or(1) + 1)
            .collect();
        let mut crossing: Vec<Vec<usize>> = vec![Vec::new(); levels.len()];
        for (index, link) in links.iter().enumerate() {
            crossing[node_column[link.from]].push(index);
        }
        let gaps: Vec<Gap> = crossing
            .iter()
            .map(|gap| route(gap, &links, &row))
            .collect();
        let mut column_x = Vec::with_capacity(levels.len());
        let mut x = 0;
        for (column, width) in column_width.iter().enumerate() {
            column_x.push(x);
            x += width + gaps[column].lanes + 4;
        }
        let width = match levels.len() {
            0 => 0,
            columns => column_x[columns - 1] + column_width[columns - 1],
        };

        // The strokes. A line leaves a task one space after its name, or a
        // waypoint where its column starts; it enters a task through an
        // arrow followed by a space, or a waypoint where its column starts.
        let start = |node: usize| {
            if node < count {
                column_x[node_column[node]] + label(node) + 1
            } else {
                column_x[node_column[node]]
            }
        };
        let mut strokes = Vec::new();
        let mut arrows = Vec::new();
        for (column, gap) in gaps.iter().enumerate().take(levels.len().saturating_sub(1)) {
            let first_lane = column_x[column] + column_width[column] + 1;
            let arrow = column_x[column + 1] - 2;
            let end = |node: usize| {
                if node < count {
                    arrow
                } else {
                    column_x[column + 1]
                }
            };
            for bundle in &gap.bundles {
                let lane = first_lane + bundle.lane;
                let link = |index: usize| &links[bundle.links[index]];
                let all = || {
                    let mut all: Vec<u32> = bundle
                        .links
                        .iter()
                        .flat_map(|&index| links[index].edges.iter().copied())
                        .collect();
                    all.sort_unstable();
                    all.dedup();
                    all
                };
                match bundle.kind {
                    Kind::Straight => {
                        let only = link(0);
                        strokes.push(across(
                            row[only.from],
                            start(only.from),
                            end(only.to),
                            &only.edges,
                        ));
                    }
                    Kind::Trunk => {
                        let from = link(0).from;
                        strokes.push(across(row[from], start(from), lane, &all()));
                        for &index in &bundle.links {
                            let link = &links[index];
                            strokes.push(down(lane, row[from], row[link.to], &link.edges));
                            strokes.push(across(row[link.to], lane, end(link.to), &link.edges));
                        }
                    }
                    Kind::Collector => {
                        let to = link(0).to;
                        for &index in &bundle.links {
                            let link = &links[index];
                            strokes.push(across(
                                row[link.from],
                                start(link.from),
                                lane,
                                &link.edges,
                            ));
                            strokes.push(down(lane, row[link.from], row[to], &link.edges));
                        }
                        strokes.push(across(row[to], lane, end(to), &all()));
                    }
                    Kind::Single => {
                        let only = link(0);
                        strokes.push(across(row[only.from], start(only.from), lane, &only.edges));
                        strokes.push(down(lane, row[only.from], row[only.to], &only.edges));
                        strokes.push(across(row[only.to], lane, end(only.to), &only.edges));
                    }
                }
            }
            // One arrow head in front of each task this gap leads to.
            let mut entering: BTreeMap<usize, Vec<u32>> = BTreeMap::new();
            for bundle in &gap.bundles {
                for &index in &bundle.links {
                    let link = &links[index];
                    if link.to < count {
                        entering.entry(link.to).or_default().extend(&link.edges);
                    }
                }
            }
            for (task, edges) in entering {
                arrows.push(Arrow {
                    row: row[task],
                    x: arrow,
                    edges,
                });
            }
        }
        strokes.retain(|stroke| match stroke.shape {
            Shape::Across { left, right, .. } => left < right,
            Shape::Down { top, bottom, .. } => top < bottom,
        });

        let columns = order
            .iter()
            .map(|column| {
                column
                    .iter()
                    .copied()
                    .filter(|&node| node < count)
                    .collect()
            })
            .collect();
        Self {
            at: (0..count)
                .map(|task| (row[task], column_x[column_of[task]]))
                .collect(),
            edges,
            names,
            column_of,
            columns,
            column_x,
            needs,
            dependents,
            arrows,
            strokes,
            width,
            height,
        }
    }

    /// The lines inside a window, as one entry per cell, row by row: the
    /// directions in which a line leaves the cell, and whether it belongs
    /// to a dependency marked in `hot`. Names and arrow heads are left out.
    pub(crate) fn raster(&self, window: &Window, hot: &[bool]) -> Vec<(u8, bool)> {
        let mut cells = vec![(0, false); window.width * window.height];
        if cells.is_empty() {
            return cells;
        }
        let (right_edge, bottom_edge) = (window.x + window.width, window.y + window.height);
        let mut mark = |x: usize, y: usize, bits: u8, is_hot: bool| {
            let cell = &mut cells[(y - window.y) * window.width + (x - window.x)];
            cell.0 |= bits;
            cell.1 |= is_hot;
        };
        for stroke in &self.strokes {
            let is_hot = stroke.edges.iter().any(|&edge| hot[edge as usize]);
            match stroke.shape {
                Shape::Across { row, left, right } => {
                    if row < window.y || row >= bottom_edge {
                        continue;
                    }
                    for x in left.max(window.x)..=right.min(right_edge - 1) {
                        let bits =
                            (if x > left { LEFT } else { 0 }) | (if x < right { RIGHT } else { 0 });
                        mark(x, row, bits, is_hot);
                    }
                }
                Shape::Down { x, top, bottom } => {
                    if x < window.x || x >= right_edge {
                        continue;
                    }
                    for y in top.max(window.y)..=bottom.min(bottom_edge - 1) {
                        let bits =
                            (if y > top { UP } else { 0 }) | (if y < bottom { DOWN } else { 0 });
                        mark(x, y, bits, is_hot);
                    }
                }
            }
        }
        cells
    }

    /// The whole picture as text, with `mark` in front of every name.
    #[cfg(test)]
    pub(crate) fn text(&self, mark: impl Fn(usize) -> char) -> String {
        let window = Window {
            x: 0,
            y: 0,
            width: self.width,
            height: self.height,
        };
        let cells = self.raster(&window, &vec![false; self.edges.len()]);
        let mut grid: Vec<Vec<char>> = cells
            .chunks(self.width.max(1))
            .map(|row| row.iter().map(|&(bits, _)| glyph(bits)).collect())
            .collect();
        for arrow in &self.arrows {
            grid[arrow.row][arrow.x] = '▶';
        }
        for (task, &(row, x)) in self.at.iter().enumerate() {
            let label = format!("{} {}", mark(task), self.names[task]);
            for (offset, character) in label.chars().enumerate() {
                grid[row][x + offset] = character;
            }
        }
        let lines: Vec<String> = grid
            .into_iter()
            .map(|row| row.into_iter().collect::<String>().trim_end().to_string())
            .collect();
        lines.join("\n")
    }
}

/// The box-drawing character for a cell left by a line in these directions.
pub(crate) fn glyph(bits: u8) -> char {
    // Indexed by the directions: up 1, down 2, left 4, right 8.
    const GLYPHS: [char; 16] = [
        ' ', '│', '│', '│', '─', '┘', '┐', '┤', '─', '└', '┌', '├', '─', '┴', '┬', '┼',
    ];
    GLYPHS[usize::from(bits & (UP | DOWN | LEFT | RIGHT))]
}

fn across(row: usize, left: usize, right: usize, edges: &[u32]) -> Stroke {
    Stroke {
        shape: Shape::Across { row, left, right },
        edges: edges.to_vec(),
    }
}

fn down(x: usize, from: usize, to: usize, edges: &[u32]) -> Stroke {
    Stroke {
        shape: Shape::Down {
            x,
            top: from.min(to),
            bottom: from.max(to),
        },
        edges: edges.to_vec(),
    }
}

/// Sorts a column by the average position of each node's neighbours in
/// the next column over (nodes without any stay where they are), and
/// records the new positions.
fn reorder(column: &mut [usize], neighbours: &[Vec<usize>], position: &mut [usize]) {
    let key = |node: usize| match neighbours[node].as_slice() {
        [] => position[node] as f64,
        around => around.iter().map(|&n| position[n] as f64).sum::<f64>() / around.len() as f64,
    };
    let mut keyed: Vec<(f64, usize, usize)> = column
        .iter()
        .map(|&node| (key(node), position[node], node))
        .collect();
    keyed.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    for (index, (slot, (_, _, node))) in column.iter_mut().zip(keyed).enumerate() {
        *slot = node;
        position[node] = index;
    }
}

/// The lower median, if there is anything.
fn median(values: impl Iterator<Item = usize>) -> Option<usize> {
    let mut values: Vec<usize> = values.collect();
    values.sort_unstable();
    values.get(values.len().saturating_sub(1) / 2).copied()
}

/// Groups the links of one gap into bundles and gives each a lane.
fn route(gap: &[usize], links: &[Link], row: &[usize]) -> Gap {
    let mut bundles = Vec::new();
    let mut by_source: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for &index in gap {
        by_source.entry(links[index].from).or_default().push(index);
    }
    let mut alone = BTreeMap::<usize, Vec<usize>>::new();
    for (_, group) in by_source {
        if group.len() > 1 {
            bundles.push(Bundle {
                kind: Kind::Trunk,
                links: group,
                lane: 0,
            });
        } else {
            alone.entry(links[group[0]].to).or_default().push(group[0]);
        }
    }
    for (_, group) in alone {
        let kind = match group.as_slice() {
            [only] if row[links[*only].from] == row[links[*only].to] => Kind::Straight,
            [_] => Kind::Single,
            _ => Kind::Collector,
        };
        bundles.push(Bundle {
            kind,
            links: group,
            lane: 0,
        });
    }

    // Lanes: first fit by span, which uses as few as interval graphs allow.
    let span = |bundle: &Bundle| {
        let rows = bundle
            .links
            .iter()
            .flat_map(|&index| [row[links[index].from], row[links[index].to]]);
        let (top, bottom) = rows.fold((usize::MAX, 0), |(top, bottom), r| {
            (top.min(r), bottom.max(r))
        });
        (top, bottom)
    };
    let mut waiting: Vec<(usize, usize, usize)> = bundles
        .iter()
        .enumerate()
        .filter(|(_, bundle)| bundle.kind != Kind::Straight)
        .map(|(index, bundle)| {
            let (top, bottom) = span(bundle);
            (top, bottom, index)
        })
        .collect();
    waiting.sort_unstable();
    let mut lane_ends: Vec<usize> = Vec::new();
    for (top, bottom, index) in waiting {
        let lane = lane_ends
            .iter()
            .position(|&end| end < top)
            .unwrap_or_else(|| {
                lane_ends.push(0);
                lane_ends.len() - 1
            });
        lane_ends[lane] = bottom;
        bundles[index].lane = lane;
    }
    Gap {
        lanes: lane_ends.len(),
        bundles,
    }
}

#[cfg(test)]
mod tests {
    use super::super::backend::TaskState;
    use super::*;

    /// A picture of tasks given as (name, needs), with their levels
    /// computed as the store does: one more than the deepest need.
    fn picture(tasks: &[(&str, &[&str])]) -> Picture {
        fn depth(rows: &[TaskRow], task: usize) -> usize {
            let deepest = rows[task].needs.iter().map(|&need| depth(rows, need) + 1);
            deepest.max().unwrap_or(0)
        }
        let index = |name: &str| tasks.iter().position(|(task, _)| *task == name).unwrap();
        let rows: Vec<TaskRow> = tasks
            .iter()
            .map(|(name, needs)| TaskRow {
                name: (*name).to_owned(),
                state: TaskState::Blocked,
                command: String::new(),
                waiting: None,
                needs: needs.iter().map(|need| index(need)).collect(),
                attempts: Vec::new(),
            })
            .collect();
        let mut levels: Vec<Vec<usize>> = Vec::new();
        for task in 0..rows.len() {
            let level = depth(&rows, task);
            if levels.len() <= level {
                levels.resize(level + 1, Vec::new());
            }
            levels[level].push(task);
        }
        Picture::new(&rows, &levels)
    }

    fn text(picture: &Picture) -> String {
        picture.text(|_| '·')
    }

    #[test]
    fn a_chain_is_a_straight_line() {
        let picture = picture(&[("a", &[]), ("b", &["a"]), ("c", &["b"])]);
        assert_eq!(text(&picture), "· a ──▶ · b ──▶ · c");
        assert_eq!(picture.height, 1);
    }

    #[test]
    fn a_fan_out_and_in_share_their_lanes() {
        let picture = picture(&[
            ("end", &["one", "three", "two"]),
            ("one", &["start"]),
            ("start", &[]),
            ("three", &["start"]),
            ("two", &["start"]),
        ]);
        let expected = [
            "         ┌─▶ · one ───┐",
            "· start ─┼─▶ · three ─┼─▶ · end",
            "         └─▶ · two ───┘",
        ];
        assert_eq!(text(&picture), expected.join("\n"));
    }

    #[test]
    fn a_dependency_that_skips_a_level_goes_around_names() {
        // The shape of the vetrina example.
        let picture = picture(&[
            ("arricchisci", &["scarica"]),
            ("controlla", &["scarica"]),
            ("modello-a", &["arricchisci", "pulisci"]),
            ("modello-b", &["arricchisci", "pulisci"]),
            ("pubblica", &["report"]),
            ("pulisci", &["scarica"]),
            ("report", &["controlla", "modello-a", "modello-b"]),
            ("scarica", &[]),
        ]);
        let expected = [
            "           ┌─▶ · arricchisci ─┬┬─▶ · modello-a ─┐",
            "· scarica ─┼─▶ · pulisci ─────┴┴─▶ · modello-b ─┼─▶ · report ──▶ · pubblica",
            "           └─▶ · controlla ─────────────────────┘",
        ];
        assert_eq!(text(&picture), expected.join("\n"));
    }

    #[test]
    fn reordering_removes_a_crossing() {
        // By name, x would face a and y would face b: the lines would cross.
        let picture = picture(&[("a", &[]), ("b", &[]), ("x", &["b"]), ("y", &["a"])]);
        assert_eq!(text(&picture), "· a ──▶ · y\n· b ──▶ · x");
    }

    #[test]
    fn long_names_are_cut() {
        let long = "a-name-much-longer-than-the-width-allowed";
        let picture = picture(&[(long, &[]), ("next", &[long])]);
        assert_eq!(picture.names[0].chars().count(), NAME_WIDTH);
        assert!(picture.names[0].ends_with('…'));
        assert!(text(&picture).ends_with("──▶ · next"), "{}", text(&picture));
    }

    #[test]
    fn the_highlight_follows_one_dependency_through_waypoints() {
        // c needs a two levels away, b needs a next door.
        let picture = picture(&[("a", &[]), ("b", &["a"]), ("c", &["a", "b"])]);
        let a_to_c = picture
            .edges
            .iter()
            .position(|&edge| edge == (0, 2))
            .unwrap();
        let mut hot = vec![false; picture.edges.len()];
        hot[a_to_c] = true;
        let window = Window {
            x: 0,
            y: 0,
            width: picture.width,
            height: picture.height,
        };
        let cells = picture.raster(&window, &hot);
        let (row_c, x_c) = picture.at[2];
        let (row_b, x_b) = picture.at[1];
        // The cell just before c's arrow is lit; the one before b's is not.
        let lit = |row: usize, x: usize| cells[row * picture.width + x].1;
        assert!(lit(row_c, x_c - 3), "{}", text(&picture));
        assert!(!lit(row_b, x_b - 3), "{}", text(&picture));
    }

    #[test]
    fn an_empty_run_has_an_empty_picture() {
        let picture = Picture::new(&[], &[]);
        assert_eq!((picture.width, picture.height), (0, 0));
        assert!(
            picture
                .raster(
                    &Window {
                        x: 0,
                        y: 0,
                        width: 0,
                        height: 0
                    },
                    &[]
                )
                .is_empty()
        );
    }
}
