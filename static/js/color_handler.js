// Color management for vector sources
export class ColorHandler {
  constructor() {
    this.LIGHT = 'light';
    this.DARK = 'dark';

    this.colorLists = {};

    this.colorLists[this.LIGHT] = [
      'FC49A3', 'CC66FF', '66CCFF', '66FFCC', '00FF00', 'FFCC66',
      'FF6666', 'FF0000', 'FF8000', 'FFFF66', '00FFFF',
    ];

    this.colorLists[this.DARK] = [
      'C71585', '663399', '4682B4', '7B68EE', '228B22', 'DAA520',
      'D2691E', 'B22222', 'FF8C00', 'FFD700', '003366',
    ];

    this.availableColors = {};
    for (const [key, colorList] of Object.entries(this.colorLists)) {
      this.availableColors[key] = [...colorList];
    }
  }

  minColorLength() {
    return Math.min(...Object.values(this.colorLists).map(list => list.length));
  }

  hasAvailableColors() {
    const key = Object.keys(this.colorLists)[0];
    return this.availableColors[key].length > 0;
  }

  assignColors() {
    if (!this.hasAvailableColors()) {
      throw new Error('No colors available');
    }

    const assigned = {};
    for (const [key, colorList] of Object.entries(this.colorLists)) {
      assigned[key] = this.availableColors[key].shift();
    }
    return assigned;
  }

  releaseColors(colors) {
    for (const key of Object.keys(this.colorLists)) {
      this.availableColors[key].push(colors[key]);
    }
  }

}
